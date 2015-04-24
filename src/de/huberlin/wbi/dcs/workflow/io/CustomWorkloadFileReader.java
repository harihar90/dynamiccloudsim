package de.huberlin.wbi.dcs.workflow.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.ParameterException;
import org.cloudbus.cloudsim.util.WorkloadFileReader;
import org.cloudbus.cloudsim.util.WorkloadModel;

import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;

public class CustomWorkloadFileReader extends LogFileReader {
	@Override
	protected void fillDataStructures(int userId, String filePath, boolean fileNames, boolean kernelTime, Workflow workflow) {
		try {
			WorkloadModel workLoad = new WorkloadFileReader("src" + File.separator
					 + "test" + File.separator + "LCG.swf.gz", 1);
			List<Cloudlet> cloudlets = workLoad.generateWorkload();
			for(Cloudlet c : cloudlets) {
				
				Task task = new Task("", "", workflow, userId, cloudletId, c.getCloudletLength(), 
									0, 0, c.getNumberOfPes(), c.getCloudletFileSize(), c.getCloudletOutputSize(), 
									utilizationModel, utilizationModel, utilizationModel);
				taskIdToTask.put((long)cloudletId, task);
				cloudletId++;
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}

}
