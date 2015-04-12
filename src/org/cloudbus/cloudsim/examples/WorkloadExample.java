package org.cloudbus.cloudsim.examples;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.util.WorkloadFileReader;
import org.cloudbus.cloudsim.util.WorkloadModel;

public class WorkloadExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			WorkloadModel workLoad = new WorkloadFileReader("src" + File.separator
									 + "test" + File.separator + "LCG.swf.gz", 1);
			List<Cloudlet> cloudlets = workLoad.generateWorkload();
			for(Cloudlet cloudlet : cloudlets)
			{
				System.out.println(cloudlet.getCloudletLength());
			}
			System.out.println("Number of cloudlets " + cloudlets.size());
		}
		catch(FileNotFoundException e)
		{
			System.out.println(e);
		}
	}

}
