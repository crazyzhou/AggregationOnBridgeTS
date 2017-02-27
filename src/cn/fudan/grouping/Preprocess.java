package cn.fudan.grouping;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import cn.fudan.tools.util.NewGenerate;

public class Preprocess {

	ArrayList<String> sList;

	public Preprocess(String dir) throws Exception {
		File directory = new File(dir);
		if (directory.isDirectory()) {
			 File[] list = directory.listFiles();
			 sList = new ArrayList<>();
			 for (File file : list) {
				 try {
					 StringBuilder sb = new StringBuilder();
					 BufferedReader br = new BufferedReader(new FileReader(file));
					 String tmpString = null;
					 while ((tmpString = br.readLine()) != null) {
						 sb.append(tmpString);
					 }
					 br.close();
					 sList.add(sb.toString());
					 NewGenerate gen = new NewGenerate();
					 gen.generate(sb.toString());
					 
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			 }
		}
	}

}
