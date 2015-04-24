package org.cloudbus.cloudsim.examples;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.distributions.ContinuousDistribution;
import org.cloudbus.cloudsim.util.WorkloadFileReader;
import org.cloudbus.cloudsim.util.WorkloadModel;

import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.VmAllocationPolicyRandom;
import de.huberlin.wbi.dcs.examples.Parameters;

public class WorkloadExample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			WorkloadModel workLoad = new WorkloadFileReader("src" + File.separator
									 + "test" + File.separator + "LCG.swf.gz", 1);
			List<Cloudlet> cloudlets = workLoad.generateWorkload();
			for(Cloudlet cloudlet : cloudlets)
			{
				System.out.println(cloudlet.getCloudletOutputSize());
			}
			System.out.println("Number of cloudlets " + cloudlets.size());
		}
		catch(FileNotFoundException e)
		{
			System.out.println(e);
		}
	}

	private VmAllocationPolicyRandom vmPolicy;
	
	// all numbers in 1000 (e.g. kb/s)
		public Datacenter createDatacenter(String name) {
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
		}

}
