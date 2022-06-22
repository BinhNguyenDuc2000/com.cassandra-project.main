package compressor;

import java.util.Arrays;
import java.util.zip.Deflater;

import org.apache.commons.codec.binary.Base64;

public class Compressor {
	public static String Compress(String input) {
		Deflater deflater = new Deflater();
		try {
			deflater.setInput(input.getBytes("UTF-8"));
			deflater.finish();
			byte[] outputBytes = new byte[1024*128];
			int deflate_size = deflater.deflate(outputBytes);
			deflater.end();
			return Base64.encodeBase64String(Arrays.copyOf(outputBytes, deflate_size));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		return null;
		
	}

}
