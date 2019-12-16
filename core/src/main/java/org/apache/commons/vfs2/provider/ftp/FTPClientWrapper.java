/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.commons.vfs2.provider.ftp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;

/**
 * A wrapper to the FTPClient to allow automatic reconnect on connection loss.<br />
 * I decided to not to use eg. noop() to determine the state of the connection to avoid
 * unnecesary server round-trips.
 * @author <a href="http://commons.apache.org/vfs/team-list.html">Commons VFS team</a>
 */
class FTPClientWrapper implements FtpClient
{
    private final GenericFileName root;
    private final FileSystemOptions fileSystemOptions;

    private FTPClient ftpClient;

    FTPClientWrapper(final GenericFileName root, final FileSystemOptions fileSystemOptions) throws FileSystemException
    {
        this.root = root;
        this.fileSystemOptions = fileSystemOptions;
        getFtpClient(); // fail-fast
    }

    public GenericFileName getRoot()
    {
        return root;
    }

    public FileSystemOptions getFileSystemOptions()
    {
        return fileSystemOptions;
    }

    private FTPClient createClient() throws FileSystemException
    {
        final GenericFileName rootName = getRoot();

        UserAuthenticationData authData = null;
        try
        {
            authData = UserAuthenticatorUtils.authenticate(fileSystemOptions, FtpFileProvider.AUTHENTICATOR_TYPES);

            return FtpClientFactory.createConnection(rootName.getHostName(),
                rootName.getPort(),
                UserAuthenticatorUtils.getData(authData, UserAuthenticationData.USERNAME,
                        UserAuthenticatorUtils.toChar(rootName.getUserName())),
                UserAuthenticatorUtils.getData(authData, UserAuthenticationData.PASSWORD,
                        UserAuthenticatorUtils.toChar(rootName.getPassword())),
                rootName.getPath(),
                getFileSystemOptions());
        }
        finally
        {
            UserAuthenticatorUtils.cleanup(authData);
        }
    }

    private FTPClient getFtpClient() throws FileSystemException
    {
        if (ftpClient == null)
        {
            ftpClient = createClient();
        }

        return ftpClient;
    }

    public boolean isConnected() throws FileSystemException
    {
        return ftpClient != null && ftpClient.isConnected();
    }

    public void disconnect() throws IOException
    {
        try
        {
            getFtpClient().disconnect();
        }
        finally
        {
            ftpClient = null;
        }
    }

    // If the specified path exists as a file or a directory return it, if not
    // return null - this means the path doesn't exist
    public FTPFile getFileOrDirectory(String fileOrDirectoryPath) throws IOException {
        try {
            return doGetFileOrDirectory(fileOrDirectoryPath);
        }
        catch (IOException e) {
            disconnect();
            return doGetFileOrDirectory(fileOrDirectoryPath);
        }
    }

    private FTPFile doGetFileOrDirectory(String fileOrDirectoryPath) throws IOException {

        if ((fileOrDirectoryPath == null) || (fileOrDirectoryPath.length() == 0)) {
            return null;
        }

        // BIG ASSUMPTION: if path includes a . then we assume it is more likely to be a file than a directory
        // Based on this we choose the most efficient means to determine the details given it is likely
        // to be one or the other

        boolean likelyToBeFile = (fileOrDirectoryPath.indexOf('.') > -1);

        if (likelyToBeFile) {
            return goGetFileOrDirectoryWhenExpectingAFile(fileOrDirectoryPath);
        }
        else {
            return goGetFileOrDirectoryWhenExpectingADirectory(fileOrDirectoryPath);
        }
    }

    private FTPFile goGetFileOrDirectoryViaNonSpacePrefix(String fileOrDirectoryPath) throws IOException {

        // If we're ending in a '/' we really shouldn't be here
        if (fileOrDirectoryPath.endsWith("/")) {
            return null;
        }

        // If the path after the last slash doesn't contain a space then we can't really help you anymore
        String suffix = fileOrDirectoryPath.substring(fileOrDirectoryPath.lastIndexOf("/") + 1);
        if (suffix.indexOf(" ") == -1) {
            return null;
        }

        // If the path before the last slash contain's a space then we can't help you either
        String prefix = fileOrDirectoryPath.substring(0, fileOrDirectoryPath.lastIndexOf("/") + 1);
        if (prefix.indexOf(" ") > -1) {
            return null;
        }

        // Do a wildcard listing without any spaces
        String subPath = fileOrDirectoryPath.substring(0, fileOrDirectoryPath.indexOf(" "));
        subPath = subPath + "*";

        FTPFile[] ftpFiles = getFtpClient().listFiles(subPath);

        // Error code indicates file or directory does not exist
        if (FTPReply.FILE_ACTION_NOT_TAKEN == getFtpClient().getReplyCode()) {
            return null;
        }

        // If we got zero results then we don't have anything
        if ((ftpFiles == null) || (ftpFiles.length == 0)) {
            return null;
        }

        // If we got one result, we need to check the name to ensure it is what we were after!
        if (ftpFiles.length == 1) {
            // Check file entry is valid and name of it is valid
            FTPFile ftpFile = ftpFiles[0];
            if ((ftpFile == null) || (ftpFile.getName() == null) || (ftpFile.getName().length() == 0)) {
                return null;
            }
            // We match on the name after any possible initial relative path
            if (fileOrDirectoryPath.endsWith(ftpFile.getName())) {
                return ftpFile;
            }
            // We didn't find anything valid or matching...
            return null;
        }
        // Oh no... we have a wildcard match of other items....
        for (int i = 0; i < ftpFiles.length; i++) {
            FTPFile ftpFile = ftpFiles[i];
            // Only check match if valid entry
            if ((ftpFile == null) || (ftpFile.getName() == null) || (ftpFile.getName().length() == 0)) {
                continue;
            }
            // We match on the name after any possible initial relative path
            if (fileOrDirectoryPath.endsWith(ftpFile.getName())) {
                return ftpFile;
            }
        }
        // We got no match... this is a problem...
        return null;
    }

    private FTPFile goGetFileOrDirectoryWhenExpectingAFile(String fileOrDirectoryPath) throws IOException {

        FTPFile[] files = getFtpClient().listFiles(fileOrDirectoryPath);

        // Error code indicates file or directory does not exist
        if (FTPReply.FILE_ACTION_NOT_TAKEN == getFtpClient().getReplyCode()) {
            return null;
        }

        // More than one result indicates a directory
        if ((files != null) && (files.length > 1)) {
            return doGetDirectory(fileOrDirectoryPath);
        }

        // Zero results indicates an empty directory
        // OR
        // an annoying server which doesn't support LS with files containing spaces (thanks Akamai)
        if ((files == null) || (files.length == 0)) {
            return goGetFileOrDirectoryViaNonSpacePrefix(fileOrDirectoryPath);
        }

        // So we have 1 entry returned... check if it is a valid entry
        FTPFile ftpFile = files[0];
        if ((ftpFile == null) || (ftpFile.getName() == null) || (ftpFile.getName().length() == 0)) {
            return null;
        }

        // One result could be a file or a directory containing a single file or directory
        // If the result is directory it is definitely a sub-directory in the directory
        if (ftpFile.isDirectory()) {
            return doGetDirectory(fileOrDirectoryPath);
        }

        // So now we either have a file which is the one we were after, or it is a file
        // inside the directory we were interested in
        // If the file has a different name then what we listed, then it is a file
        // in the directory we were after
        // We match on the name after any possible initial relative path
        if (!fileOrDirectoryPath.endsWith(ftpFile.getName())) {
            return doGetDirectory(fileOrDirectoryPath);
        }

        // NOW: edge case is that the file inside is named the same as the directory -
        // we have no way of knowing this unless we try to change to the directory
        // if this fails, we have a file which we are interested in
        String workingDirectory = getFtpClient().printWorkingDirectory();
        if (!getFtpClient().changeWorkingDirectory(fileOrDirectoryPath)) {
            // We couldn't change, so we have a file we're after!
            return ftpFile;
        }

        // We could change, so we have a directory, but we need to change back first...
        if (!getFtpClient().changeWorkingDirectory(workingDirectory)) {
            throw new FileSystemException("vfs.provider.ftp.wrapper/change-work-directory-back.error", workingDirectory);
        }
        return doGetDirectory(fileOrDirectoryPath);
    }

    private FTPFile goGetFileOrDirectoryWhenExpectingADirectory(String fileOrDirectoryPath) throws IOException {

        // Need to do a wildcard search to get the directory rather than the contents of the directory
        String directoryPath = fileOrDirectoryPath + "*";

        FTPFile[] ftpFiles = getFtpClient().listFiles(directoryPath);

        // Error code indicates file or directory does not exist
        if (FTPReply.FILE_ACTION_NOT_TAKEN == getFtpClient().getReplyCode()) {
            return null;
        }

        // If we got zero results then we don't have such a directory (or even a file)
        // OR
        // an annoying server which doesn't support LS with files containing spaces (thanks Akamai)
        if ((ftpFiles == null) || (ftpFiles.length == 0)) {
            return goGetFileOrDirectoryViaNonSpacePrefix(fileOrDirectoryPath);
        }

        // If we got one result we will take it if we have a name match
        // It will either be a file or a directory - we don't care
        if (ftpFiles.length == 1) {
            // Check file entry is valid and name of it is valid
            FTPFile ftpFile = ftpFiles[0];
            if ((ftpFile == null) || (ftpFile.getName() == null) || (ftpFile.getName().length() == 0)) {
                return null;
            }
            // We match on the name after any possible initial relative path
            if (fileOrDirectoryPath.endsWith(ftpFile.getName())) {
                return ftpFile;
            }
            // We didn't find anything valid or matching the wildcard...
            return null;
        }

        // Oh no... we have a wildcard match of other folders....
        for (int i = 0; i < ftpFiles.length; i++) {
            FTPFile ftpFile = ftpFiles[i];
            // Only check match if valid entry
            if ((ftpFile == null) || (ftpFile.getName() == null) || (ftpFile.getName().length() == 0)) {
                continue;
            }
            // We match on the name after any possible initial relative path
            if (fileOrDirectoryPath.endsWith(ftpFile.getName())) {
                return ftpFile;
            }
        }

        // We got no match for a wildcard
        return null;
    }

    private FTPFile doGetDirectory(String fileOrDirectoryPath) throws IOException {

        // NOTE: Need to do a wildcard search to get the directory rather than the contents of the directory
        String directoryPath = fileOrDirectoryPath + "*";

        FTPFile[] ftpFiles = getFtpClient().listFiles(directoryPath);

        // We got no match... this is a problem...
        if ((ftpFiles == null) || (ftpFiles.length == 0)) {
            return null;
        }

        // If we got one result, we need to check the name to ensure it is what we were after!
        if (ftpFiles.length == 1) {
            // Check file entry is valid and name of it is valid
            FTPFile ftpFile = ftpFiles[0];
            if ((ftpFile == null) || (ftpFile.getName() == null) || (ftpFile.getName().length() == 0)) {
                return null;
            }
            // We match on the name after any possible initial relative path
            if (fileOrDirectoryPath.endsWith(ftpFile.getName())) {
                return ftpFile;
            }
            // We didn't find anything valid or matching...
            return null;
        }
        // Oh no... we have a wildcard match of other folders....
        for (int i = 0; i < ftpFiles.length; i++) {
            FTPFile ftpFile = ftpFiles[i];
            // Only check match if valid entry
            if ((ftpFile == null) || (ftpFile.getName() == null) || (ftpFile.getName().length() == 0)) {
                continue;
            }
            // We match on the name after any possible initial relative path
            if (fileOrDirectoryPath.endsWith(ftpFile.getName())) {
                return ftpFile;
            }
        }
        // We got no match... this is a problem...
        return null;
    }

    public FTPFile[] listFiles(String relPath) throws IOException
    {
        try
        {
            // VFS-210: return getFtpClient().listFiles(relPath);
            FTPFile[] files = listFilesInDirectory(relPath);
            return files;
        }
        catch (IOException e)
        {
            disconnect();

            FTPFile[] files = listFilesInDirectory(relPath);
            return files;
        }
    }

    private FTPFile[] listFilesInDirectory(String relPath) throws IOException
    {
        FTPFile[] files;

        // VFS-307: no check if we can simply list the files, this might fail if there are spaces in the path
        files = getFtpClient().listFiles(relPath);
        if (FTPReply.isPositiveCompletion(getFtpClient().getReplyCode()))
        {
            return files;
        }

        // VFS-307: now try the hard way by cd'ing into the directory, list and cd back
        // if VFS is required to fallback here the user might experience a real bad FTP performance
        // as then every list requires 4 ftp commands.
        String workingDirectory = null;
        if (relPath != null)
        {
            workingDirectory = getFtpClient().printWorkingDirectory();
            if (!getFtpClient().changeWorkingDirectory(relPath))
            {
                return null;
            }
        }

        files = getFtpClient().listFiles();

        if (relPath != null && !getFtpClient().changeWorkingDirectory(workingDirectory))
        {
            throw new FileSystemException("vfs.provider.ftp.wrapper/change-work-directory-back.error",
                    workingDirectory);
        }
        return files;
    }

    public boolean removeDirectory(String relPath) throws IOException
    {
        try
        {
            return getFtpClient().removeDirectory(relPath);
        }
        catch (IOException e)
        {
            disconnect();
            return getFtpClient().removeDirectory(relPath);
        }
    }

    public boolean deleteFile(String relPath) throws IOException
    {
        try
        {
            return getFtpClient().deleteFile(relPath);
        }
        catch (IOException e)
        {
            disconnect();
            return getFtpClient().deleteFile(relPath);
        }
    }

    public boolean rename(String oldName, String newName) throws IOException
    {
        try
        {
            return getFtpClient().rename(oldName, newName);
        }
        catch (IOException e)
        {
            disconnect();
            return getFtpClient().rename(oldName, newName);
        }
    }

    public boolean makeDirectory(String relPath) throws IOException
    {
        try
        {
            return getFtpClient().makeDirectory(relPath);
        }
        catch (IOException e)
        {
            disconnect();
            return getFtpClient().makeDirectory(relPath);
        }
    }

    public boolean completePendingCommand() throws IOException
    {
        if (ftpClient != null)
        {
            return getFtpClient().completePendingCommand();
        }

        return true;
    }

    public InputStream retrieveFileStream(String relPath) throws IOException
    {
        try
        {
            return getFtpClient().retrieveFileStream(relPath);
        }
        catch (IOException e)
        {
            disconnect();
            return getFtpClient().retrieveFileStream(relPath);
        }
    }

    public InputStream retrieveFileStream(String relPath, long restartOffset) throws IOException
    {
        try
        {
            FTPClient client = getFtpClient();
            client.setRestartOffset(restartOffset);
            return client.retrieveFileStream(relPath);
        }
        catch (IOException e)
        {
            disconnect();

            FTPClient client = getFtpClient();
            client.setRestartOffset(restartOffset);
            return client.retrieveFileStream(relPath);
        }
    }

    public OutputStream appendFileStream(String relPath) throws IOException
    {
        try
        {
            return getFtpClient().appendFileStream(relPath);
        }
        catch (IOException e)
        {
            disconnect();
            return getFtpClient().appendFileStream(relPath);
        }
    }

    public OutputStream storeFileStream(String relPath) throws IOException
    {
        try
        {
            return getFtpClient().storeFileStream(relPath);
        }
        catch (IOException e)
        {
            disconnect();
            return getFtpClient().storeFileStream(relPath);
        }
    }

    public boolean abort() throws IOException
    {
        try
        {
            // imario@apache.org: 2005-02-14
            // it should be better to really "abort" the transfer, but
            // currently I didnt manage to make it work - so lets "abort" the hard way.
            // return getFtpClient().abort();

            disconnect();
            return true;
        }
        catch (IOException e)
        {
            disconnect();
        }
        return true;
    }

    public String getReplyString() throws IOException
    {
        return getFtpClient().getReplyString();
    }
}
