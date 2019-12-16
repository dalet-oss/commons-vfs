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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileNotFolderException;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.UriParser;
import org.apache.commons.vfs2.util.FileObjectUtils;
import org.apache.commons.vfs2.util.Messages;
import org.apache.commons.vfs2.util.MonitorInputStream;
import org.apache.commons.vfs2.util.MonitorOutputStream;
import org.apache.commons.vfs2.util.RandomAccessMode;

/**
 * An FTP file.
 *
 * @author <a href="http://commons.apache.org/vfs/team-list.html">Commons VFS team</a>
 */
public class FtpFileObject extends AbstractFileObject
{
    private static final Map<String, FTPFile> EMPTY_FTP_FILE_MAP =
        Collections.unmodifiableMap(new TreeMap<String, FTPFile>());
    private static final FTPFile UNKNOWN = new FTPFile();

    private final Log log = LogFactory.getLog(FtpFileObject.class);
    private final FtpFileSystem ftpFs;
    private final String relPath;

    // Cached info
    private FTPFile fileInfo;
    private Map<String, FTPFile> children;
    private FileObject linkDestination;

    private boolean inRefresh;

    protected FtpFileObject(final AbstractFileName name,
                            final FtpFileSystem fileSystem,
                            final FileName rootName)
        throws FileSystemException
    {
        super(name, fileSystem);
        ftpFs = fileSystem;
        String relPath = UriParser.decode(rootName.getRelativeName(name));
        if (".".equals(relPath))
        {
            // do not use the "." as path against the ftp-server
            // e.g. the uu.net ftp-server do a recursive listing then
            // this.relPath = UriParser.decode(rootName.getPath());
            // this.relPath = ".";
            this.relPath = null;
        }
        else
        {
            this.relPath = relPath;
        }
    }

    /**
     * Called by child file objects, to locate their ftp file info.
     *
     * @param name  the filename in its native form ie. without uri stuff (%nn)
     * @param flush recreate children cache
     */
    private FTPFile getChildFile(final String name, final boolean flush) throws IOException
    {
        /* If we should flush cached children, clear our children map unless
                 * we're in the middle of a refresh in which case we've just recently
                 * refreshed our children. No need to do it again when our children are
                 * refresh()ed, calling getChildFile() for themselves from within
                 * getInfo(). See getChildren(). */
        if ((flush && !inRefresh) || (children == null)) {
            return getFileOrDirectory(name);
        }

        return children.get(name);
    }

    private FTPFile getFileOrDirectory(String name) throws IOException {
        final FtpClient client = ftpFs.getClient();
        try {
            final String path = fileInfo != null && fileInfo.isSymbolicLink()
                ? getFileSystem().getFileSystemManager().
                    resolveName(getParent().getName(), fileInfo.getLink()).getPath()
                : relPath;
            String filePath = name;
            if (path != null) {
                filePath = path + "/" + name;
            }

            return client.getFileOrDirectory(filePath);
        }
        finally {
            ftpFs.putClient(client);
        }
    }

    /**
     * Fetches the children of this file, if not already cached.
     */
    private void doGetChildren() throws IOException
    {
        if (children != null)
        {
            return;
        }

        final FtpClient client = ftpFs.getClient();
        try
        {
            final String path = fileInfo != null && fileInfo.isSymbolicLink()
                ? getFileSystem().getFileSystemManager().
                    resolveName(getParent().getName(), fileInfo.getLink()).getPath()
                : relPath;
            final FTPFile[] tmpChildren = client.listFiles(path);
            if (tmpChildren == null || tmpChildren.length == 0)
            {
                children = EMPTY_FTP_FILE_MAP;
            }
            else
            {
                children = new TreeMap<String, FTPFile>();

                // Remove '.' and '..' elements
                for (int i = 0; i < tmpChildren.length; i++)
                {
                    final FTPFile child = tmpChildren[i];
                    if (child == null)
                    {
                        if (log.isDebugEnabled())
                        {
                            log.debug(Messages.getString("vfs.provider.ftp/invalid-directory-entry.debug",
                                new Object[]
                                    {
                                        new Integer(i), relPath
                                    }));
                        }
                        continue;
                    }
                    if (!".".equals(child.getName())
                        && !"..".equals(child.getName()))
                    {
                        children.put(child.getName(), child);
                    }
                }
            }
        }
        finally
        {
            ftpFs.putClient(client);
        }
    }

    /**
     * Attaches this file object to its file resource.
     */
    @Override
    protected void doAttach()
        throws IOException
    {
        // Get the parent folder to find the info for this file
        // VFS-210 getInfo(false);
    }

    /**
     * Fetches the info for this file.
     */
    private void getInfo(boolean flush) throws IOException
    {
        final FtpFileObject parent = (FtpFileObject) FileObjectUtils.getAbstractFileObject(getParent());
        FTPFile newFileInfo;
        if (parent != null)
        {
            newFileInfo = parent.getChildFile(UriParser.decode(getName().getBaseName()), flush);
        }
        else
        {
            // Assume the root is a directory and exists
            newFileInfo = new FTPFile();
            newFileInfo.setType(FTPFile.DIRECTORY_TYPE);
        }

        if (newFileInfo == null)
        {
            this.fileInfo = UNKNOWN;
        }
        else
        {
            this.fileInfo = newFileInfo;
        }
    }

    /**
     * @throws FileSystemException if an error occurs.
     */
    @Override
    public void refresh() throws FileSystemException
    {
        if (!inRefresh)
        {
            try
            {
                inRefresh = true;
                super.refresh();

                synchronized (getFileSystem())
                {
                    this.fileInfo = null;
                }

                /* VFS-210
                try
                {
                    // this will tell the parent to recreate its children collection
                    getInfo(true);
                }
                catch (IOException e)
                {
                    throw new FileSystemException(e);
                }
                */
            }
            finally
            {
                inRefresh = false;
            }
        }
    }

    /**
     * Detaches this file object from its file resource.
     */
    @Override
    protected void doDetach()
    {
        synchronized (getFileSystem())
        {
            this.fileInfo = null;
            children = null;
        }
    }

    /**
     * Called when the children of this file change.
     */
    @Override
    protected void onChildrenChanged(FileName child, FileType newType)
    {
        if (children != null && newType.equals(FileType.IMAGINARY))
        {
            try
            {
                children.remove(UriParser.decode(child.getBaseName()));
            }
            catch (FileSystemException e)
            {
                throw new RuntimeException(e.getMessage());
            }
        }
        else
        {
            // if child was added we have to rescan the children
            // TODO - get rid of this
            children = null;
        }
    }

    /**
     * Called when the type or content of this file changes.
     */
    @Override
    protected void onChange() throws IOException
    {
        children = null;

        if (getType().equals(FileType.IMAGINARY))
        {
            // file is deleted, avoid server lookup
            synchronized (getFileSystem())
            {
                this.fileInfo = UNKNOWN;
            }
            return;
        }

        getInfo(true);
    }

    /**
     * Determines the type of the file, returns null if the file does not
     * exist.
     */
    @Override
    protected FileType doGetType()
        throws Exception
    {
        // VFS-210
        synchronized (getFileSystem())
        {
            if (this.fileInfo == null)
            {
                getInfo(false);
            }

            if (this.fileInfo == UNKNOWN)
            {
                return FileType.IMAGINARY;
            }
            else if (this.fileInfo.isDirectory())
            {
                return FileType.FOLDER;
            }
            else if (this.fileInfo.isFile())
            {
                return FileType.FILE;
            }
            else if (this.fileInfo.isSymbolicLink())
            {
                return getLinkDestination().getType();
            }
        }

        throw new FileSystemException("vfs.provider.ftp/get-type.error", getName());
    }

    private FileObject getLinkDestination() throws FileSystemException
    {
        if (linkDestination == null)
        {
            final String path;
            synchronized (getFileSystem())
            {
                path = this.fileInfo.getLink();
            }
            FileName relativeTo = getName().getParent();
            if (relativeTo == null)
            {
                relativeTo = getName();
            }
            FileName linkDestinationName = getFileSystem().getFileSystemManager().resolveName(relativeTo, path);
            linkDestination = getFileSystem().resolveFile(linkDestinationName);
        }

        return linkDestination;
    }

    @Override
    protected FileObject[] doListChildrenResolved() throws Exception
    {
        synchronized (getFileSystem())
        {
            if (this.fileInfo != null && this.fileInfo.isSymbolicLink())
            {
                return getLinkDestination().getChildren();
            }
        }

        return null;
    }

    /**
     * Returns the file's list of children.
     *
     * @return The list of children
     * @throws FileSystemException If there was a problem listing children
     * @see AbstractFileObject#getChildren()
     * @since 2.0
     */
    @Override
    public FileObject[] getChildren() throws FileSystemException
    {
        try
        {
            if (doGetType() != FileType.FOLDER)
            {
                throw new FileNotFolderException(getName());
            }
        }
        catch (Exception ex)
        {
            throw new FileNotFolderException(getName(), ex);
        }


        try
        {
            /* Wrap our parent implementation, noting that we're refreshing so
             * that we don't refresh() ourselves and each of our parents for
             * each children. Note that refresh() will list children. Meaning,
             * if if this file has C children, P parents, there will be (C * P)
             * listings made with (C * (P + 1)) refreshes, when there should
             * really only be 1 listing and C refreshes. */

            this.inRefresh = true;
            return super.getChildren();
        }
        finally
        {
            this.inRefresh = false;
        }
    }

    /**
     * Lists the children of the file.
     */
    @Override
    protected String[] doListChildren()
        throws Exception
    {
        // List the children of this file
        doGetChildren();

        // VFS-210
        if (children == null)
        {
            return null;
        }

        // TODO - get rid of this children stuff
        final String[] childNames = new String[children.size()];
        int childNum = -1;
        Iterator<FTPFile> iterChildren = children.values().iterator();
        while (iterChildren.hasNext())
        {
            childNum++;
            final FTPFile child = iterChildren.next();
            childNames[childNum] = child.getName();
        }

        return UriParser.encode(childNames);
    }

    /**
     * Deletes the file.
     */
    @Override
    protected void doDelete() throws Exception
    {
        synchronized (getFileSystem())
        {
            final boolean ok;
            final FtpClient ftpClient = ftpFs.getClient();
            try
            {
                if (this.fileInfo.isDirectory())
                {
                    ok = ftpClient.removeDirectory(relPath);
                }
                else
                {
                    ok = ftpClient.deleteFile(relPath);
                }
            }
            finally
            {
                ftpFs.putClient(ftpClient);
            }

            if (!ok)
            {
                throw new FileSystemException("vfs.provider.ftp/delete-file.error", getName());
            }
            this.fileInfo = null;
            children = EMPTY_FTP_FILE_MAP;
        }
    }

    /**
     * Renames the file
     */
    @Override
    protected void doRename(FileObject newfile) throws Exception
    {
        synchronized (getFileSystem())
        {
            final boolean ok;
            final FtpClient ftpClient = ftpFs.getClient();
            try
            {
                String oldName = getName().getPath();
                String newName = newfile.getName().getPath();
                ok = ftpClient.rename(oldName, newName);
            }
            finally
            {
                ftpFs.putClient(ftpClient);
            }

            if (!ok)
            {
                throw new FileSystemException("vfs.provider.ftp/rename-file.error",
                        new Object[]{getName().toString(), newfile});
            }
            this.fileInfo = null;
            children = EMPTY_FTP_FILE_MAP;
        }
    }

    /**
     * Creates this file as a folder.
     */
    @Override
    protected void doCreateFolder()
        throws Exception
    {
        final boolean ok;
        final FtpClient client = ftpFs.getClient();
        try
        {
            ok = client.makeDirectory(relPath);
        }
        finally
        {
            ftpFs.putClient(client);
        }

        if (!ok)
        {
            throw new FileSystemException("vfs.provider.ftp/create-folder.error", getName());
        }
    }

    /**
     * Returns the size of the file content (in bytes).
     */
    @Override
    protected long doGetContentSize() throws Exception
    {
        synchronized (getFileSystem())
        {
            if (this.fileInfo.isSymbolicLink())
            {
                return getLinkDestination().getContent().getSize();
            }
            else
            {
                return this.fileInfo.getSize();
            }
        }
    }

    /**
     * get the last modified time on an ftp file
     *
     * @see org.apache.commons.vfs2.provider.AbstractFileObject#doGetLastModifiedTime()
     */
    @Override
    protected long doGetLastModifiedTime() throws Exception
    {
        synchronized (getFileSystem())
        {
            if (this.fileInfo.isSymbolicLink())
            {
                return getLinkDestination().getContent().getLastModifiedTime();
            }
            else
            {
                Calendar timestamp = this.fileInfo.getTimestamp();
                if (timestamp == null)
                {
                    return 0L;
                }
                else
                {
                    return (timestamp.getTime().getTime());
                }
            }
        }
    }

    /**
     * Creates an input stream to read the file content from.
     */
    @Override
    protected InputStream doGetInputStream() throws Exception
    {
        final FtpClient client = ftpFs.getClient();
        try
        {
            final InputStream instr = client.retrieveFileStream(relPath);
            // VFS-210
            if (instr == null)
            {
                throw new FileNotFoundException(getName().toString());
            }
            return new FtpInputStream(client, instr);
        }
        catch (Exception e)
        {
            ftpFs.putClient(client);
            throw e;
        }
    }

    @Override
    protected RandomAccessContent doGetRandomAccessContent(final RandomAccessMode mode) throws Exception
    {
        return new FtpRandomAccessContent(this, mode);
    }

    /**
     * Creates an output stream to write the file content to.
     */
    @Override
    protected OutputStream doGetOutputStream(boolean bAppend)
        throws Exception
    {
        final FtpClient client = ftpFs.getClient();
        try
        {
            OutputStream out = null;
            if (bAppend)
            {
                out = client.appendFileStream(relPath);
            }
            else
            {
                out = client.storeFileStream(relPath);
            }

            if (out == null)
            {
                throw new FileSystemException("vfs.provider.ftp/output-error.debug", new Object[]
                    {
                        this.getName(),
                        client.getReplyString()
                    });
            }

            return new FtpOutputStream(client, out);
        }
        catch (Exception e)
        {
            ftpFs.putClient(client);
            throw e;
        }
    }

    String getRelPath()
    {
        return relPath;
    }

    FtpInputStream getInputStream(long filePointer) throws IOException
    {
        final FtpClient client = ftpFs.getClient();
        try
        {
            final InputStream instr = client.retrieveFileStream(relPath, filePointer);
            if (instr == null)
            {
                throw new FileSystemException("vfs.provider.ftp/input-error.debug", new Object[]
                    {
                        this.getName(),
                        client.getReplyString()
                    });
            }
            return new FtpInputStream(client, instr);
        }
        catch (IOException e)
        {
            ftpFs.putClient(client);
            throw e;
        }
    }

    /**
     * An InputStream that monitors for end-of-file.
     */
    class FtpInputStream
        extends MonitorInputStream
    {
        private final FtpClient client;

        public FtpInputStream(final FtpClient client, final InputStream in)
        {
            super(in);
            this.client = client;
        }

        void abort() throws IOException
        {
            client.abort();
            close();
        }

        /**
         * Called after the stream has been closed.
         */
        @Override
        protected void onClose() throws IOException
        {
            final boolean ok;
            try
            {
                ok = client.completePendingCommand();
            }
            finally
            {
                ftpFs.putClient(client);
            }

            if (!ok)
            {
                throw new FileSystemException("vfs.provider.ftp/finish-get.error", getName());
            }
        }
    }

    /**
     * An OutputStream that monitors for end-of-file.
     */
    private class FtpOutputStream
        extends MonitorOutputStream
    {
        private final FtpClient client;

        public FtpOutputStream(final FtpClient client, final OutputStream outstr)
        {
            super(outstr);
            this.client = client;
        }

        /**
         * Called after this stream is closed.
         */
        @Override
        protected void onClose() throws IOException
        {
            final boolean ok;
            try
            {
                ok = client.completePendingCommand();
            }
            finally
            {
                ftpFs.putClient(client);
            }

            if (!ok)
            {
                throw new FileSystemException("vfs.provider.ftp/finish-put.error", getName());
            }
        }
    }
}
