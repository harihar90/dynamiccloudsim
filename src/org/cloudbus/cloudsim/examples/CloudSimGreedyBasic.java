package org.cloudbus.cloudsim.examples;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.GreedyDataCenterBroker;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.cloudbus.cloudsim.util.WorkloadFileReader;
import org.cloudbus.cloudsim.util.WorkloadModel;

import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.VmAllocationPolicyRandom;
import de.huberlin.wbi.dcs.examples.Parameters;

public class CloudSimGreedyBasic {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			int num_user = 1;   // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = true;  // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);
			Datacenter dc= createDatacenter("dc0");
			DatacenterBroker broker = createBroker(0);
			//Fourth step: Create one virtual machine for each broker/user
			List<Vm> vmlist1 = new ArrayList<Vm>();
			

			//VM description
			int vmid = 0;
			int mips = 250;
			long size = 10000; //image size (MB)
			int ram = 4096; //vm memory (MB)
			long bw = 1000;
			int pesNumber = 1; //number of cpus
			String vmm = "Xen"; //VMM name

			//create two VMs: the first one belongs to user1

			Vm vm1 = new Vm(vmid, broker.getId(), mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
			vmlist1.add(vm1);
			WorkloadModel workLoad = new WorkloadFileReader("src" + File.separator
									 + "test" + File.separator + "LCG.swf.gz", 1);
			List<Cloudlet> cloudlets = createCloudlet(broker.getId(), 1, 0);;
			

			broker.submitVmList(vmlist1);
			broker.submitCloudletList(cloudlets);
			
			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			// Final step: Print results when simulation is over
			List<Cloudlet> newList1 = broker.getCloudletReceivedList();
		

			CloudSim.stopSimulation();

			Log.print("=============> User "+0+"    ");
			printCloudletList(newList1);

			CloudSim.stopSimulation();

			//Print the debt of each user to each datacenter
			dc.printDebts();
			System.out.println("Number of cloudlets " + cloudlets.size());
		}
		catch(FileNotFoundException e)
		{
			System.out.println(e);
		}
	}
	private static List<Cloudlet> createCloudlet(int userId, int cloudlets, int idShift){
		// Creates a container to store Cloudlets
		LinkedList<Cloudlet> list = new LinkedList<Cloudlet>();

		//cloudlet parameters
		long length = 40000;
		long fileSize = 300;
		long outputSize = 300;
		int pesNumber = 1;
		UtilizationModel utilizationModel = new UtilizationModelFull();

		Cloudlet[] cloudlet = new Cloudlet[cloudlets];

		for(int i=0;i<cloudlets;i++){
			cloudlet[i] = new Cloudlet(idShift + i, length, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
			// setting the owner of these Cloudlets
			cloudlet[i].setUserId(userId);
			list.add(cloudlet[i]);
			
		}

		return list;
	}

	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 */
	private static void printCloudletList(List<Cloudlet> list) {
		int size = list.size();
		Cloudlet cloudlet;
		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
				"Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			cloudlet = list.get(i);
			Log.print(indent + cloudlet.getCloudletId() + indent + indent);

			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS){
				Log.print("SUCCESS");

				Log.printLine( indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId() +
						indent + indent + dft.format(cloudlet.getActualCPUTime()) + indent + indent + dft.format(cloudlet.getExecStartTime())+
						indent + indent + dft.format(cloudlet.getFinishTime()));
			}
		}

	}
	//We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
		//to the specific rules of the simulated scenario
		private static DatacenterBroker createBroker(int id){

			DatacenterBroker broker = null;
			try {
				//broker = new GreedyDataCenterBroker("Broker"+id, 10,1,vmPolicy);
				broker=new DatacenterBroker("Broker"+id);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
			return broker;
		}
	private static VmAllocationPolicyRandom vmPolicy;
	private static Datacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store one or more
		//    Machines
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
		//    create a list to store these PEs before creating
		//    a Machine.
		List<Pe> peList1 = new ArrayList<Pe>();

		int mips = 100;

		// 3. Create PEs and add these into the list.
		//for a quad-core machine, a list of 4 PEs is required:
		peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		
		//Another list, for a dual-core machine
		List<Pe> peList2 = new ArrayList<Pe>();
		mips=mips*2;
		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));
		
		//4. Create Hosts with its id and list of PEs and add them to the list of machines
		int hostId=0;
		int ram = 16384; //host memory (MB)
		long storage = 1000000; //host storage
		int bw = 10000;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList1,

    				new VmSchedulerSpaceShared(peList1)

    			)
    		); // This is our first machine

		hostId++;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList2,
    				new VmSchedulerTimeSharedOverSubscription(peList2)
    			)
    		); // Second machine

		// 5. Create a DatacenterCharacteristics object that stores the
		//    properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone
		//    and its price (G$/Pe time unit).
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.1;	// the cost of using storage in this resource
		double costPerBw = 0.1;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicyRandom(hostList,0), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	// all numbers in 1000 (e.g. kb/s)
	/*	public static Datacenter createDatacenter(String name) {
			Random numGen;
			numGen = Parameters.numGen;
			List<DynamicHost> hostList = new ArrayList<DynamicHost>();
			int hostId = 0;
			long storage = 1024 * 1024;

			int ram = (int) (2 * 1024 * Parameters.nCusPerCoreAMD2218HE * Parameters.nCoresAMD2218HE);
			for (int i = 0; i < Parameters.nAMD2218HE; i++) {
				double mean = 1d;
				double dev = Parameters.bwHeterogeneityCV;
				ContinuousDistribution dist = Parameters.getDistribution(
						Parameters.bwHeterogeneityDistribution, mean,
						Parameters.bwHeterogeneityAlpha,
						Parameters.bwHeterogeneityBeta, dev,
						Parameters.bwHeterogeneityShape,
						Parameters.bwHeterogeneityLocation,
						Parameters.bwHeterogeneityShift,
						Parameters.bwHeterogeneityMin,
						Parameters.bwHeterogeneityMax,
						Parameters.bwHeterogeneityPopulation);
				long bwps = 0;
				while (bwps <= 0) {
					bwps = (long) (dist.sample() * Parameters.bwpsPerPe);
				}
				mean = 1d;
				dev = Parameters.ioHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.ioHeterogeneityDistribution, mean,
						Parameters.ioHeterogeneityAlpha,
						Parameters.ioHeterogeneityBeta, dev,
						Parameters.ioHeterogeneityShape,
						Parameters.ioHeterogeneityLocation,
						Parameters.ioHeterogeneityShift,
						Parameters.ioHeterogeneityMin,
						Parameters.ioHeterogeneityMax,
						Parameters.ioHeterogeneityPopulation);
				long iops = 0;
				while (iops <= 0) {
					iops = (long) (long) (dist.sample() * Parameters.iopsPerPe);
				}
				mean = 1d;
				dev = Parameters.cpuHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.cpuHeterogeneityDistribution, mean,
						Parameters.cpuHeterogeneityAlpha,
						Parameters.cpuHeterogeneityBeta, dev,
						Parameters.cpuHeterogeneityShape,
						Parameters.cpuHeterogeneityLocation,
						Parameters.cpuHeterogeneityShift,
						Parameters.cpuHeterogeneityMin,
						Parameters.cpuHeterogeneityMax,
						Parameters.cpuHeterogeneityPopulation);
				long mips = 0;
				while (mips <= 0) {
					mips = (long) (long) (dist.sample() * Parameters.mipsPerCoreAMD2218HE);
				}
				if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
					bwps *= Parameters.stragglerPerformanceCoefficient;
					iops *= Parameters.stragglerPerformanceCoefficient;
					mips *= Parameters.stragglerPerformanceCoefficient;
				}
				hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage,
						Parameters.nCusPerCoreAMD2218HE, Parameters.nCoresAMD2218HE, mips));
			}

			ram = (int) (2 * 1024 * Parameters.nCusPerCoreXeon5507 * Parameters.nCoresXeon5507);
			for (int i = 0; i < Parameters.nXeon5507; i++) {
				double mean = 1d;
				double dev = Parameters.bwHeterogeneityCV;
				ContinuousDistribution dist = Parameters.getDistribution(
						Parameters.bwHeterogeneityDistribution, mean,
						Parameters.bwHeterogeneityAlpha,
						Parameters.bwHeterogeneityBeta, dev,
						Parameters.bwHeterogeneityShape,
						Parameters.bwHeterogeneityLocation,
						Parameters.bwHeterogeneityShift,
						Parameters.bwHeterogeneityMin,
						Parameters.bwHeterogeneityMax,
						Parameters.bwHeterogeneityPopulation);
				long bwps = 0;
				while (bwps <= 0) {
					bwps = (long) (dist.sample() * Parameters.bwpsPerPe);
				}
				mean = 1d;
				dev = Parameters.ioHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.ioHeterogeneityDistribution, mean,
						Parameters.ioHeterogeneityAlpha,
						Parameters.ioHeterogeneityBeta, dev,
						Parameters.ioHeterogeneityShape,
						Parameters.ioHeterogeneityLocation,
						Parameters.ioHeterogeneityShift,
						Parameters.ioHeterogeneityMin,
						Parameters.ioHeterogeneityMax,
						Parameters.ioHeterogeneityPopulation);
				long iops = 0;
				while (iops <= 0) {
					iops = (long) (long) (dist.sample() * Parameters.iopsPerPe);
				}
				mean = 1d;
				dev = Parameters.cpuHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.cpuHeterogeneityDistribution, mean,
						Parameters.cpuHeterogeneityAlpha,
						Parameters.cpuHeterogeneityBeta, dev,
						Parameters.cpuHeterogeneityShape,
						Parameters.cpuHeterogeneityLocation,
						Parameters.cpuHeterogeneityShift,
						Parameters.cpuHeterogeneityMin,
						Parameters.cpuHeterogeneityMax,
						Parameters.cpuHeterogeneityPopulation);
				long mips = 0;
				while (mips <= 0) {
					mips = (long) (long) (dist.sample() * Parameters.mipsPerCoreXeon5507);
				}
				if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
					bwps *= Parameters.stragglerPerformanceCoefficient;
					iops *= Parameters.stragglerPerformanceCoefficient;
					mips *= Parameters.stragglerPerformanceCoefficient;
				}
				hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage,
						Parameters.nCusPerCoreXeon5507, Parameters.nCoresXeon5507, mips));
			}

			ram = (int) (2 * 1024 * Parameters.nCusPerCoreXeonE5430 * Parameters.nCoresXeonE5430);
			for (int i = 0; i < Parameters.nXeonE5430; i++) {
				double mean = 1d;
				double dev = Parameters.bwHeterogeneityCV;
				ContinuousDistribution dist = Parameters.getDistribution(
						Parameters.bwHeterogeneityDistribution, mean,
						Parameters.bwHeterogeneityAlpha,
						Parameters.bwHeterogeneityBeta, dev,
						Parameters.bwHeterogeneityShape,
						Parameters.bwHeterogeneityLocation,
						Parameters.bwHeterogeneityShift,
						Parameters.bwHeterogeneityMin,
						Parameters.bwHeterogeneityMax,
						Parameters.bwHeterogeneityPopulation);
				long bwps = 0;
				while (bwps <= 0) {
					bwps = (long) (dist.sample() * Parameters.bwpsPerPe);
				}
				mean = 1d;
				dev = Parameters.ioHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.ioHeterogeneityDistribution, mean,
						Parameters.ioHeterogeneityAlpha,
						Parameters.ioHeterogeneityBeta, dev,
						Parameters.ioHeterogeneityShape,
						Parameters.ioHeterogeneityLocation,
						Parameters.ioHeterogeneityShift,
						Parameters.ioHeterogeneityMin,
						Parameters.ioHeterogeneityMax,
						Parameters.ioHeterogeneityPopulation);
				long iops = 0;
				while (iops <= 0) {
					iops = (long) (long) (dist.sample() * Parameters.iopsPerPe);
				}
				mean = 1d;
				dev = Parameters.cpuHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.cpuHeterogeneityDistribution, mean,
						Parameters.cpuHeterogeneityAlpha,
						Parameters.cpuHeterogeneityBeta, dev,
						Parameters.cpuHeterogeneityShape,
						Parameters.cpuHeterogeneityLocation,
						Parameters.cpuHeterogeneityShift,
						Parameters.cpuHeterogeneityMin,
						Parameters.cpuHeterogeneityMax,
						Parameters.cpuHeterogeneityPopulation);
				long mips = 0;
				while (mips <= 0) {
					mips = (long) (long) (dist.sample() * Parameters.mipsPerCoreXeonE5430);
				}
				if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
					bwps *= Parameters.stragglerPerformanceCoefficient;
					iops *= Parameters.stragglerPerformanceCoefficient;
					mips *= Parameters.stragglerPerformanceCoefficient;
				}
				hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage,
						Parameters.nCusPerCoreXeonE5430, Parameters.nCoresXeonE5430, mips));
			}
			
			ram = (int) (2 * 1024 * Parameters.nCusPerCoreXeonE5645 * Parameters.nCoresXeonE5645);
			for (int i = 0; i < Parameters.nXeonE5645; i++) {
				double mean = 1d;
				double dev = Parameters.bwHeterogeneityCV;
				ContinuousDistribution dist = Parameters.getDistribution(
						Parameters.bwHeterogeneityDistribution, mean,
						Parameters.bwHeterogeneityAlpha,
						Parameters.bwHeterogeneityBeta, dev,
						Parameters.bwHeterogeneityShape,
						Parameters.bwHeterogeneityLocation,
						Parameters.bwHeterogeneityShift,
						Parameters.bwHeterogeneityMin,
						Parameters.bwHeterogeneityMax,
						Parameters.bwHeterogeneityPopulation);
				long bwps = 0;
				while (bwps <= 0) {
					bwps = (long) (dist.sample() * Parameters.bwpsPerPe);
				}
				mean = 1d;
				dev = Parameters.ioHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.ioHeterogeneityDistribution, mean,
						Parameters.ioHeterogeneityAlpha,
						Parameters.ioHeterogeneityBeta, dev,
						Parameters.ioHeterogeneityShape,
						Parameters.ioHeterogeneityLocation,
						Parameters.ioHeterogeneityShift,
						Parameters.ioHeterogeneityMin,
						Parameters.ioHeterogeneityMax,
						Parameters.ioHeterogeneityPopulation);
				long iops = 0;
				while (iops <= 0) {
					iops = (long) (long) (dist.sample() * Parameters.iopsPerPe);
				}
				mean = 1d;
				dev = Parameters.cpuHeterogeneityCV;
				dist = Parameters.getDistribution(
						Parameters.cpuHeterogeneityDistribution, mean,
						Parameters.cpuHeterogeneityAlpha,
						Parameters.cpuHeterogeneityBeta, dev,
						Parameters.cpuHeterogeneityShape,
						Parameters.cpuHeterogeneityLocation,
						Parameters.cpuHeterogeneityShift,
						Parameters.cpuHeterogeneityMin,
						Parameters.cpuHeterogeneityMax,
						Parameters.cpuHeterogeneityPopulation);
				long mips = 0;
				while (mips <= 0) {
					mips = (long) (long) (dist.sample() * Parameters.mipsPerCoreXeonE5645);
				}
				if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
					bwps *= Parameters.stragglerPerformanceCoefficient;
					iops *= Parameters.stragglerPerformanceCoefficient;
					mips *= Parameters.stragglerPerformanceCoefficient;
				}
				hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage,
						Parameters.nCusPerCoreXeonE5645, Parameters.nCoresXeonE5645, mips));
			}

			String arch = "x86";
			String os = "Linux";
			String vmm = "Xen";
			double time_zone = 10.0;
			double cost = 3.0;
			double costPerMem = 0.05;
			double costPerStorage = 0.001;
			double costPerBw = 0.0;
			LinkedList<Storage> storageList = new LinkedList<Storage>();

			DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
					arch, os, vmm, hostList, time_zone, cost, costPerMem,
					costPerStorage, costPerBw);

			Datacenter datacenter = null;
			vmPolicy=new VmAllocationPolicyRandom(hostList, Parameters.seed++);
			
			try {
				datacenter = new Datacenter(name, characteristics,vmPolicy
						,storageList, 0);
			} catch (Exception e) {
				e.printStackTrace();
			}

			return datacenter;
		}*/
	/*private static Datacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		//    our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList = new ArrayList<Pe>();

		int mips=1000;

		// 3. Create PEs and add these into a list.
		peList.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating

		//4. Create Host with its id and list of PEs and add them to the list of machines
		int hostId=0;
		int ram = 2048; //host memory (MB)
		long storage = 1000000; //host storage
		int bw = 10000;


		//in this example, the VMAllocatonPolicy in use is SpaceShared. It means that only one VM
		//is allowed to run on each Pe. As each Host has only one Pe, only one VM can run on each Host.
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList,
    				new VmSchedulerSpaceShared(peList)
    			)
    		); // This is our first machine

		// 5. Create a DatacenterCharacteristics object that stores the
		//    properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone
		//    and its price (G$/Pe time unit).
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xen";
		double time_zone = 10.0;         // time zone this resource located
		double cost = 3.0;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		double costPerStorage = 0.001;	// the cost of using storage in this resource
		double costPerBw = 0.0;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}*/


}
