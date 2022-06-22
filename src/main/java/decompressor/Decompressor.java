package decompressor;

import java.util.Arrays;
import java.util.zip.Inflater;

import org.apache.commons.codec.binary.Base64;

public class Decompressor {
	public static String decompress(String input) {
		Inflater inflater = new Inflater();
		try {
			
			inflater.setInput(Base64.decodeBase64(input));
			byte[] outputBytes = new byte[1024*128];
			
			int inflateSize = inflater.inflate(outputBytes);
			inflater.end();
			return new String(Arrays.copyOf(outputBytes, inflateSize), "UTF-8");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(1);
		}
		return null;
	}
}
