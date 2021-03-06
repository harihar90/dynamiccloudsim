package de.huberlin.wbi.dcs.examples;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.GreedyDataCenterBroker;
import org.cloudbus.cloudsim.GreedyDataCenterBroker_Migration;
import org.cloudbus.cloudsim.GreedyDataCenterBroker_Opportunistic;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;

import com.sun.org.apache.xalan.internal.xsltc.runtime.Parameter;

import de.huberlin.wbi.dcs.CloudletSchedulerGreedyDivided;
import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.DynamicModel;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.VmAllocationPolicyRandom;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.Workflow;
import de.huberlin.wbi.dcs.workflow.io.AlignmentTraceFileReader;
import de.huberlin.wbi.dcs.workflow.io.CuneiformLogFileReader;
import de.huberlin.wbi.dcs.workflow.io.CustomWorkloadFileReader;
import de.huberlin.wbi.dcs.workflow.io.DaxFileReader;
import de.huberlin.wbi.dcs.workflow.io.MontageTraceFileReader;
import de.huberlin.wbi.dcs.workflow.scheduler.AbstractWorkflowScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.C2O;
import de.huberlin.wbi.dcs.workflow.scheduler.C3;
import de.huberlin.wbi.dcs.workflow.scheduler.GreedyQueueScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.HEFTScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.LATEScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.StaticRoundRobinScheduler;
import edu.isi.pegasus.planner.client.ExitCode;

public class WorkflowExample {

	public static void main(String[] args) {
		Map<Integer,Double> debtMap = new HashMap<Integer,Double>();
		Parameters.parseParameters(args);
		double[] totalRuntime= new double[Parameters.nUsers];
		try { 
			Datacenter dc = null;
			
			for (int i = 0; i < Parameters.numberOfRuns; i++) {
				WorkflowExample ex = new WorkflowExample();
				if (!Parameters.outputDatacenterEvents) {
					Log.disable();
				}
				// Initialize the CloudSim package
				int num_user = 1; // number of grid users
				Calendar calendar = Calendar.getInstance();
				boolean trace_flag = false; // mean trace events
				CloudSim.init(num_user, calendar, trace_flag);
				List<AbstractWorkflowScheduler> users= new ArrayList<AbstractWorkflowScheduler>();
			    dc=ex.createDatacenter("Datacenter"+i,i);
			    for(int j=0;j<Parameters.nUsers;j++){
				AbstractWorkflowScheduler scheduler = ex.createScheduler(i);
				ex.createVms(i, scheduler);
				Workflow workflow = buildWorkflow(scheduler);
				ex.submitWorkflow(workflow, scheduler);
				users.add(scheduler);
				}
				// Start the simulation
				CloudSim.startSimulation();
				CloudSim.stopSimulation();
				int firstUserID = users.get(0).getId();
				int timeDiff = 0;
				for(int j=0;j<Parameters.nUsers;j++){
				int nthUser = (users.get(j).getId() - firstUserID);
		
				if(nthUser % Parameters.NUM_USERS_PER_DELAY == 0 && nthUser != 0) {
					timeDiff += (Parameters.TIME_QUANTA * Parameters.TIME_INTERVAL_USERS);
				}
				totalRuntime[j] += (users.get(j).getRuntime() - timeDiff)/60;
				double debt = (dc.getDebts().get(users.get(j).getId())==null)?0:dc.getDebts().get(users.get(j).getId());
				if(debtMap.get(users.get(j).getId())==null)
					debtMap.put(users.get(j).getId(), debt);
				else
					debtMap.put(users.get(j).getId(), debt+debtMap.get(users.get(j).getId()));
				users.get(j).clear();
				
			}
				DatacenterBroker.clearCount();
				
			}
			for(int j=0;j<Parameters.nUsers;j++){
				
				System.out.println(totalRuntime[j] /Parameters.numberOfRuns +","+debtMap.get(j+3)/Parameters.numberOfRuns);
			}
			
			
			Log.printLine("Total Workload: " + Task.getTotalMi() + "mi "
					+ Task.getTotalIo() + "io " + Task.getTotalBw() + "bw");
			Log.printLine("Total VM Performance: " + DynamicHost.getTotalMi()
					+ "mips " + DynamicHost.getTotalIo() + "iops "
					+ DynamicHost.getTotalBw() + "bwps");
			Log.printLine("minimum minutes (quotient): " + Task.getTotalMi()
					/ DynamicHost.getTotalMi() / 60 + " " + Task.getTotalIo()
					/ DynamicHost.getTotalIo() / 60 + " " + Task.getTotalBw()
					/ DynamicHost.getTotalBw() / 60);
		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("The simulation has been terminated due to an unexpected error");
		}

	}

	private VmAllocationPolicyRandom[] vmPolicy= new VmAllocationPolicyRandom[Parameters.numberOfRuns];
	public AbstractWorkflowScheduler createScheduler(int i) {
		try {
			switch (Parameters.scheduler) {
			case STATIC_ROUND_ROBIN:
				return new StaticRoundRobinScheduler(
						"StaticRoundRobinScheduler", Parameters.taskSlotsPerVm);
			case LATE:
				return new LATEScheduler("LATEScheduler", Parameters.taskSlotsPerVm);
			case HEFT:
				return new HEFTScheduler("HEFTScheduler", Parameters.taskSlotsPerVm);
			case JOB_QUEUE:
				return new GreedyQueueScheduler("GreedyQueueScheduler",
						Parameters.taskSlotsPerVm);
			case C3:
				return new C3("C3", Parameters.taskSlotsPerVm);
			case C2O:
				
				if(Parameters.game==Parameters.Gaming.BASIC)
				return new GreedyDataCenterBroker("C2O",vmPolicy[i],Parameters.tVms,Parameters.nVms, Parameters.taskSlotsPerVm, i);
				
				else if(Parameters.game==Parameters.Gaming.OPPORTUNISTIC)
					return new GreedyDataCenterBroker_Opportunistic("C2O",vmPolicy[i],Parameters.tVms,Parameters.nVms, Parameters.taskSlotsPerVm, i, Parameters.recheck_interval, 0,Parameters.OPPORTUNISTIC_THRESHOLD*Parameters.STANDARD_MIPS_PER_CU );
				else if(Parameters.game==Parameters.Gaming.BASIC_WITH_MIGRATION)
					return new GreedyDataCenterBroker_Migration("C2O",vmPolicy[i],Parameters.tVms,Parameters.nVms, Parameters.taskSlotsPerVm, i, Parameters.recheck_interval, 0,Parameters.STANDARD_MIPS_PER_CU );
				else
					return new C2O("C2O",Parameters.taskSlotsPerVm, i);
			default:
				return new GreedyQueueScheduler("GreedyQueueScheduler",
						Parameters.taskSlotsPerVm);
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	
	public void createVms(int run, AbstractWorkflowScheduler scheduler) {
		// Create VMs
		List<Vm> vmlist = createVMList(scheduler.getId(), run);
		scheduler.submitVmList(vmlist);
	}

	public static Workflow buildWorkflow(AbstractWorkflowScheduler scheduler) {
		switch (Parameters.experiment) {
		case MONTAGE_TRACE_1:
			return new MontageTraceFileReader().parseLogFile(scheduler.getId(),
					"examples/montage.m17.1.trace", true, true, ".*jpg");
		case MONTAGE_TRACE_12:
			return new MontageTraceFileReader().parseLogFile(scheduler.getId(),
					"examples/montage.m17.12.trace", true, true, ".*jpg");
		case ALIGNMENT_TRACE:
			return new AlignmentTraceFileReader().parseLogFile(
					scheduler.getId(), "examples/alignment.caco.geo.chr22.trace2", true,
					true, null);
		case MONTAGE_25:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage_25.xml", true, true, null);
		case MONTAGE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Montage_1000.xml", true, true, null);
		case CYBERSHAKE_1000:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/CyberShake_1000.xml", true, true, null);
		case EPIGENOMICS_997:
			return new DaxFileReader().parseLogFile(scheduler.getId(),
					"examples/Epigenomics_997.xml", true, true, null);
		case CUNEIFORM_VARIANT_CALL:
			return new CuneiformLogFileReader().parseLogFile(scheduler.getId(),
					"examples/i1_s11756_r7_greedyQueue.log", true, true, null);
		case HETEROGENEOUS_TEST_WORKFLOW:
			return new CuneiformLogFileReader().parseLogFile(scheduler.getId(),
					"examples/heterogeneous_test_workflow.log", true, true, null);
		case CUSTOM_WORKLOAD:
			return new CustomWorkloadFileReader().parseLogFile(scheduler.getId(), 
					"examples/LCG.swf.gz", false, false, null);
		}
		return null;
	}

	public void submitWorkflow(Workflow workflow, AbstractWorkflowScheduler scheduler) {
		// Create Cloudlets and send them to Scheduler
		if (Parameters.outputWorkflowGraph) {
			workflow.visualize(1920, 1200);
		}
		scheduler.submitWorkflow(workflow);
	}

	// all numbers in 1000 (e.g. kb/s)
	public Datacenter createDatacenter(String name,int run) {
		Random numGen;
		numGen = Parameters.numGen;
		List<DynamicHost> hostList = new ArrayList<DynamicHost>();
		int hostId = 0;
		long storage = 1024 * 1024;
		long perfAverage =0;
		
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
				//mips = (long) (long) (Parameters.mipsPerCoreAMD2218HE);
			}
			//System.out.println(mips);
			
			if (numGen.nextDouble() < Parameters.likelihoodOfStraggler) {
				bwps *= Parameters.stragglerPerformanceCoefficient;
				iops *= Parameters.stragglerPerformanceCoefficient;
				mips *= Parameters.stragglerPerformanceCoefficient;
			}
			perfAverage+=mips/Parameters.nCusPerCoreAMD2218HE;
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
			perfAverage+=mips/Parameters.nCusPerCoreXeon5507;
			
			
			hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage,
					Parameters.nCusPerCoreXeon5507, Parameters.nCoresXeon5507, mips));
		}

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
			perfAverage+=mips/Parameters.nCusPerCoreXeonE5430;
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
			perfAverage+=mips/Parameters.nCusPerCoreXeonE5645;
			hostList.add(new DynamicHost(hostId++, ram, bwps, iops, storage,
					Parameters.nCusPerCoreXeonE5645, Parameters.nCoresXeonE5645, mips));
		}
		perfAverage/=(Parameters.nAMD2218HE+Parameters.nXeon5507+Parameters.nXeonE5430+Parameters.nXeonE5645);
		Parameters.STANDARD_MIPS_PER_CU=perfAverage;
		System.out.println("Average Performance:"+perfAverage);
		String arch = "x86";
		String os = "Linux";
		String vmm = "Xen";
		double time_zone = 10.0;
		double cost = 0.0025;
		double costPerMem = 0.0025;
		double costPerStorage = 0.001;
		double costPerBw = 0.0;
		LinkedList<Storage> storageList = new LinkedList<Storage>();

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
				arch, os, vmm, hostList, time_zone, cost, costPerMem,
				costPerStorage, costPerBw);

		Datacenter datacenter = null;
		vmPolicy[run]=new VmAllocationPolicyRandom(hostList, Parameters.seed++);
		
		try {
			datacenter = new Datacenter(name, characteristics,vmPolicy[run]
					,storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	public List<Vm> createVMList(int userId, int run) {

		// Creates a container to store VMs. This list is passed to the broker
		// later
		LinkedList<Vm> list = new LinkedList<Vm>();

		// VM Parameters
		long storage = 10000;
		String vmm = "Xen";
		int vmCount=0;
		// create VMs
		if(Parameters.game==Parameters.Gaming.BASIC ||Parameters.game==Parameters.Gaming.BASIC_WITH_MIGRATION )
			vmCount=Parameters.tVms;
		else
			vmCount=Parameters.nVms;
		Vm[] vm = new DynamicVm[vmCount];

		for (int i = 0; i < vmCount; i++) {
			DynamicModel dynamicModel = new DynamicModel();
			vm[i] = new DynamicVm(i, userId, Parameters.numberOfCusPerPe, Parameters.numberOfPes,
					Parameters.ram, storage, vmm, new CloudletSchedulerGreedyDivided(),
					dynamicModel, "output/run_" + run + "_vm_" + i + ".csv",
					Parameters.taskSlotsPerVm);
			list.add(vm[i]);
		}
		
		
		return list;
	}

}
