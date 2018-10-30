package testing;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import org.junit.Test;

public class PersistenceStorageTest extends TestCase {
    static TemporaryFolder temporaryFolder;

    public void setUp() {
        temporaryFolder = new TemporaryFolder();
    }

    @Test
    public void testCreateFileTest() throws IOException {
        File file = temporaryFolder.getRoot();
        File tempFolder = temporaryFolder.newFolder("Folder");
        System.out.println("Test folder: " + temporaryFolder.getRoot());
        assertTrue(tempFolder.exists());
    }
}