package br.ufc.crateus.eda.btree;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import br.ufc.crateus.eda.btree.dtypes.DataType;

public class DataSerializer<V> {
	private DataType<V> valueDT;
	private File file;
	
	static final int FILE_SIZE_OFFSET = 0;
	private long fileSize;
	
	public DataSerializer(File file, DataType<V> dt) throws IOException {
		this.file = file;
		this.valueDT = dt;
		fileSize = getFileSize();
	}
	
	V read(long offset) throws Exception {
		RandomAccessFile fileStore = new RandomAccessFile(file, "r"); 
		fileStore.seek(offset);
		V value = valueDT.read(fileStore);
		fileStore.close();
		return value;
	}
	
	void write(long offset, V val) throws Exception {
		RandomAccessFile fileStore = new RandomAccessFile(file, "rw"); 
		fileStore.seek(offset);
		valueDT.write(val, fileStore);
		
		fileSize += valueDT.size();
		fileStore.seek(FILE_SIZE_OFFSET);
		fileStore.writeLong(fileSize);
		
		fileStore.close();
	}
	
	public long append(V val) throws Exception {
		long lastPosition = fileSize; 
		write(fileSize, val);
		return lastPosition;
	}
	
	private long getFileSize() throws IOException {
		if (file.exists()) {
			RandomAccessFile fileStore = new RandomAccessFile(file, "r"); 
			fileStore.seek(FILE_SIZE_OFFSET);
			long fileSize = fileStore.readLong();
			fileStore.close();
			return fileSize;
		}
		else {
			RandomAccessFile fileStore = new RandomAccessFile(file, "rw");
			fileStore.seek(FILE_SIZE_OFFSET);
			fileStore.writeLong(8L);
			fileStore.close();
			return 0L;
		}
	}
}
