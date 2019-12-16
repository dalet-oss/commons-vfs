package org.apache.commons.vfs2.provider;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class HostFileNameParserTest {

    private HostFileNameParser hostFileNameParser;

    private final int defaultPort = 21;


    @Before
    public void doSetUp() {

        hostFileNameParser = new HostFileNameParser(defaultPort);
    }


    /**
     * Tests the <code>ExtractUserInfo method gives hostname from url</code>
     * <p/>
     * return hostname
     */

    @Test
    public void testExtractUserInfoReturnHostName() {

        StringBuilder hostNameInfo = new StringBuilder("mio@192.168.100.141:21/");
        String strHostName = hostFileNameParser.extractUserInfo(hostNameInfo);
        assertEquals(strHostName, "mio");

    }


    /**
     * Tests the <code>ExtractUserInfo method we have send encoded @ sign in username string</code>
     * <p/>
     * return hostname
     */
    @Test
    public void testExtractUserInfoWithAtTheRateSign() {

        StringBuilder hostNameInfo = new StringBuilder("mio%40ashish@192.168.100.141:21/");
        String strHostName = hostFileNameParser.extractUserInfo(hostNameInfo);
        assertEquals(strHostName, "mio%40ashish");

    }


    /**
     * Tests the <code>ExtractUserInfo method send url null</code>
     * <p/>
     * return null hostname
     */

    @Test
    public void testExtractUserInfoWithNameNull() {

        StringBuilder hostNameInfo = new StringBuilder();
        String strHostName = hostFileNameParser.extractUserInfo(hostNameInfo);
        assertNull(strHostName);

    }
}
