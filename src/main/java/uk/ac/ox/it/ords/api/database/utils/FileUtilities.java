package uk.ac.ox.it.ords.api.database.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileUtilities {

    private static Logger log = LoggerFactory.getLogger(FileUtilities.class);
    
    /**
     * Read file contents from an InputStream
     *
     * @param inputStream the required input stream
     * @return the contents of the input stream as a String
     * @throws IOException
     */
    public static String readFile(InputStream inputStream) throws IOException {
        StringWriter writer = new StringWriter();
        IOUtils.copy(inputStream, writer, "UTF-8");
        return writer.toString();
    }

    /**
     * Read file contents from a File
     *
     * @param file the required file
     * @return the contents of the file as a String
     * @throws IOException
     */
    public static byte[] readFileGetBytes(File file) throws IOException {
        return org.apache.commons.io.FileUtils.readFileToByteArray(file);
    }

    /**
     * Read file contents from a File
     *
     * @param file the required file
     * @return the contents of the input stream as an InputStream
     * @throws IOException
     */
    public static InputStream readFileGetIS(File file) throws IOException {
        InputStream is = null;

        try {
            is = new FileInputStream(file);
        }
        catch (FileNotFoundException e) {
            log.debug("Error getting file IS", e);
        }

        return is;
    }

    /**
     * saveFile
     * Saves a given input stream to a specified file
     * @param f
     * @param stream
     * @throws Exception
     */
    public static void saveFile ( File f, InputStream stream ) throws Exception {
        OutputStream out = new FileOutputStream(f);
        
        int read = 0;
        byte[] bytes = new byte[1024];
        while ((read = stream.read(bytes)) != -1) {
            out.write(bytes, 0, read);
        }
        stream.close();
        out.flush();
        out.close();

    }

    
    public static File createTemporaryDirectory() throws IOException {
    	String tempDir = "temp" + Long.toString(System.nanoTime());
    	File td = new File(getTmpDir() + "/" + tempDir);
        td.mkdirs();
        td.setWritable(true, false);
        return td;
    }
    
    public static String getTmpDir() {
        String tmpDir = System.getProperty("java.io.tmpdir");
        if (tmpDir == null) {
            log.error("Cannot find temp dir");
            return "/tmp";
        }
        return tmpDir;
    }

    
    /**
     * 
     * @param folder
     * @return
     * @throws Exception
     */
	public static File combineFilesToZip(File folder) throws Exception {
		return combineFilesToZip(Arrays.asList(folder.listFiles()));
	}
    
    /**
     * 
     * @param fileList
     * @return
     * @throws Exception
     */
    public static File combineFilesToZip(List<File> fileList) throws Exception {
        log.debug("combineFilesToZip");
        File result = null;

        if (fileList != null) {
            File source = new File(FileUtilities.getTmpDir() + "/database" + Long.toString(System.nanoTime()));
            File tmpZip = new File(FileUtilities.getTmpDir() + "/database" + Long.toString(System.nanoTime()));
            source.createNewFile();
            tmpZip.createNewFile();
//            tmpZip.delete();
//            source.renameTo(tmpZip);
            byte[] buffer = new byte[1024];
            ZipInputStream zin = new ZipInputStream(new FileInputStream(tmpZip));
            ZipOutputStream out = new ZipOutputStream(new FileOutputStream(source));

            for (File f : fileList) {
                InputStream in = new FileInputStream(f);
                out.putNextEntry(new ZipEntry(f.getName()));
                for (int read = in.read(buffer); read > -1; read = in.read(buffer)) {
                    out.write(buffer, 0, read);
                }
                out.closeEntry();
                in.close();
            }

            for (ZipEntry ze = zin.getNextEntry(); ze != null; ze = zin.getNextEntry()) {
                out.putNextEntry(ze);
                for (int read = zin.read(buffer); read > -1; read = zin.read(buffer)) {
                    out.write(buffer, 0, read);
                }
                out.closeEntry();
            }
            out.close();
            zin.close();
            tmpZip.delete();
            result = source;
        }

        return result;
    }
    
    
    /**
     * Get the file extension part of the file name if it exists
     * @param fileName
     * @return
     */
    public static String getFileExtension(String fileName ) {
    	if ( fileName.lastIndexOf('.') == -1 ) {
    		return null;
    	}
    	return fileName.substring(fileName.lastIndexOf('.')+1);
    }
    
    
    /**
     * Concatenate text files into a single file. It will separate each file using some new lines and the extension of
     * the input file.
     *
     * @param fileList a list of all text files to be concatenated
     * @return the file
     * @throws FileNotFoundException
     * @throws IOException
     */
   
    public static File concatenateFiles ( List<File> fileList) throws IOException {
    	File output = new File(FileUtilities.getTmpDir() + "/database" + Long.toString(System.nanoTime()));
    	FileUtilities.concatenateFiles(output, fileList);
    	return output;
    }
    
    
    
    
    /**
     * Concatenate text files into a single file. It will separate each file using some new lines and the extension of
     * the input file.
     *
     * @param outFile the resultant concatenated file
     * @param fileList a list of all text files to be concatenated
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void concatenateFiles(File outFile, List<File> fileList) throws IOException {
        log.debug("concatenateFiles");

        OutputStream out = new FileOutputStream(outFile);
        byte[] buf = new byte[1024];
        int b = 0;
        try {
	        for (File file : fileList) {
	            String name = file.getName();
	            /*
	             * The file name should consist of the tablename followed by _data followed by a set of random digits.
	             * If we strip that out we should just have the table
	             */
	            String tableName;
	            if ( (name == null) || (name.indexOf(fileNameDemarkation) <= 0) ) {
	            	log.error("Unable to get table name from name " + name);
	            	tableName = name;
	            }
	            else {
	            	tableName = name.substring(0, name.indexOf(fileNameDemarkation));//file.getName().
	            }
	            
	            if (log.isDebugEnabled()) {
	                log.debug("File Name:" + name);
	            }
	            out.write((tableName + "\n").getBytes());
	
	            InputStream in = new FileInputStream(file);
	            try {
		            while ((b = in.read(buf)) >= 0) {
		                out.write(buf, 0, b);
		                if (log.isDebugEnabled()) {
		                    log.debug("Writing data:" + new String(buf));
		                }
		            }
	            }
	            finally {
	            	if (in != null) {
	            		in.close();
	            	}
	            }
	            
	            out.write("\n\n\n".getBytes());
	        }
        }
        finally {
        	if (out != null) {
        		out.close();
        	}
        }

        log.debug("concatenateFiles:return");
    }
    
    
    /**
     * Recursively deletes all files and subdirectories under src.
     *
     * @param src folder to be deleted
     * @return true if all deletions were successful.
     */
    public static boolean deleteDir(File src) {
        if (src.isDirectory()) {
            String[] children = src.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(src, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        /*
         * The directory is now empty so delete it
         */
        return src.delete();
    }

    
    private static String fileNameDemarkation = "_data";

    


}
