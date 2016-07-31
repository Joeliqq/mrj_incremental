package cn.edu.neu.mitt.mrj.update;

import cn.edu.neu.mitt.mrj.importtriples.FilesImportTriples;

public class TriplesDel {
	public static void main(String[] args){
		try {
			args[11] = "2";
			FilesImportTriples.main(args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
