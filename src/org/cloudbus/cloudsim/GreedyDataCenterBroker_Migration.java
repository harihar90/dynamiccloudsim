package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.GreedyDataCenterBroker.AllocatedVM;
import org.cloudbus.cloudsim.GreedyDataCenterBroker_Opportunistic.PerformanceMonitoringTask;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import de.huberlin.wbi.dcs.CloudletSchedulerGreedyDivided;
import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.DynamicModel;
import de.huberlin.wbi.dcs.DynamicVm;
import de.huberlin.wbi.dcs.examples.Parameters;
import de.huberlin.wbi.dcs.workflow.Task;
import de.huberlin.wbi.dcs.workflow.scheduler.C2O.WienerProcessModel;

public class GreedyDataCenterBroker_Migration extends GreedyDataCenterBroker {

	private int pauseInterval;
	private int mipsThreshold;
	private Double threshold;
	private boolean cloudletsSubmittedAlready=false;
	public GreedyDataCenterBroker_Migration(String name,VmAllocationPolicy policy,int totalLimit,int perQLimit,int parameter1,int parameter2,int recheckInterval,int mipsThreshold,double threshold) throws Exception {
		super(name,policy,totalLimit,perQLimit,parameter1,parameter2);
		this.setRecheckInterval(recheckInterval);
		this.setMipsThreshold(mipsThreshold);
		this.setThreshold(threshold);
	}
	
	public int getMipsThreshold() {
		return mipsThreshold;
	}

	public void setMipsThreshold(int mipsThreshold) {
		this.mipsThreshold = mipsThreshold;
	}
	boolean monitorStarted=false;
	private Thread monitor;
	@Override
	public void clear()
	{
		monitor.stop();
	}
	@Override
	/**
	 * Process the ack received due to a request for VM creation.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */

	
	
	protected void processVmCreate(SimEvent ev) throws Exception {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];
		List<AllocatedVM> allocList= new ArrayList<AllocatedVM>();
		if (result == CloudSimTags.TRUE) {
			
			setNumRunningInstances(getNumRunningInstances()+1);
			DynamicHost host=(DynamicHost) getVmPolicy().getVmTable().get(VmList.getById(getVmList(), vmId).getUserId()+"-"+vmId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			getVmsToDatacentersMap().put(vmId, datacenterId);
			
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in Datacenter #" + datacenterId + ", Host #"
					+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId()+" with mips:"+VmList.getById(getVmsCreatedList(), vmId).getHost().getTotalMips()/VmList.getById(getVmsCreatedList(), vmId).getHost().getNumberOfPes());
			if(numRunningInstances==totalInstanceCountLimit)
			{
				vmHostMap.put(vmId,host.getId());
				Iterator<Entry<Integer,Integer>> iter= vmHostMap.entrySet().iterator();
				while(iter.hasNext())
				{
					Entry<Integer,Integer> entry=iter.next();
					allocList.add(new AllocatedVM(entry.getKey(),((DynamicHost) vmPolicy.getVmTable().get(VmList.getById(getVmList(), vmId).getUserId()+"-"+entry.getKey())).getMipsPerPe()/((DynamicHost) vmPolicy.getVmTable().get(VmList.getById(getVmList(), vmId).getUserId()+"-"+entry.getKey())).getNumberOfCusPerPe()));
				}
				Collections.sort(allocList, new Comparator<AllocatedVM>(){
					   public int compare(AllocatedVM o1, AllocatedVM o2){
					      return (int) (o1.getAvailableMips() - o2.getAvailableMips());
					   }
					});
				List<Integer> retainedVmList = new ArrayList<Integer>();
				for(int i=allocList.size()-perQuantumInstanceCount;i<allocList.size();i++)
				{
					retainedVmList.add(allocList.get(i).getId());
					
				}
				List<Integer> destroyedVmList = new ArrayList<Integer>();
				for(int i=0;i<allocList.size()-perQuantumInstanceCount;i++)
				{
					destroyedVmList.add(allocList.get(i).getId());
					
				}
				
				getVmHostMap().put(vmId,host.getId());
				submitCloudlets();
				Timer t = new Timer();

				

				monitor = new Thread(new PerformanceMonitoringTask(this, threshold,retainedVmList,destroyedVmList));
				monitor.setPriority(Thread.MAX_PRIORITY);
				monitor.start();
				
				
				Thread.sleep(1000);
				
			}
			else{
				
			 
			vmHostMap.put(vmId,host.getId());
			}
			
			
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
					+ " failed in Datacenter #" + datacenterId);
		}

		incrementVmsAcks();

		// all the requested VMs have been created
		if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
			if(!cloudletsSubmittedAlready)
			{cloudletsSubmittedAlready=true;
			
			}
		} else {
			// all the acks received, but some VMs were not created
			if (getVmsRequested() == getVmsAcks()) {
				// find id of the next datacenter that has not been tried
				for (int nextDatacenterId : getDatacenterIdsList()) {
					if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
						createVmsInDatacenter(nextDatacenterId);
						return;
					}
				}

				// all datacenters already queried
				if (getVmsCreatedList().size() > 0) { // if some vm were created
					submitCloudlets();
				} else { // no vms created. abort
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": none of the required VMs could be created. Aborting");
					finishExecution();
				}
			}
		}
	}

	public int getRecheckInterval() {
		return pauseInterval;
	}

	public void setRecheckInterval(int recheckInterval) {
		this.pauseInterval = recheckInterval;
	}

	public Double getThreshold() {
		return threshold;
	}

	public void setThreshold(double threshold2) {
		this.threshold = threshold2;
	} 
	
	public class PerformanceMonitoringTask implements Runnable {
		private GreedyDataCenterBroker_Migration broker;
		private Double meanMips;
		private List<Integer> allocList;
		private List<Integer> retainedVmList;
			public Double getMeanMips() {
			return meanMips;
		}
		public void setMeanMips(Double meanMips) {
			this.meanMips = meanMips;
		}
		HashMap<Integer,Integer> cloudletCountMap = new HashMap<Integer,Integer>();
			public PerformanceMonitoringTask(GreedyDataCenterBroker_Migration broker,Double meanMips, List<Integer> retainedVmList, List<Integer> destroyedVmList) {
				this.setBroker(broker);
				this.setMeanMips(meanMips);
				this.setRetainedVmList(retainedVmList);
				this.setAllocList(destroyedVmList);
			}
			class VmPerformanceData
			{
				int vmId;
				public int getVmId() {
					return vmId;
				}
				public void setVmId(int vmId) {
					this.vmId = vmId;
				}
				public double getCloudletsLength() {
					return cloudletsLength;
				}
				public void setCloudletsLength(double cloudletsLength) {
					this.cloudletsLength = cloudletsLength;
				}
				double cloudletsLength;
				
			}
			@Override
			public void run() {
				long j=1;
				long vmDestroyedCount=0;
				while(j++ !=Parameters.RECHECK_LIMIT){
					
					CloudSim.pauseSimulation((j)*broker.getRecheckInterval());
					
					while (true) {
						if (CloudSim.isPaused()) {
							break;
						}
						
						try {
							Thread.sleep(100);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}
					
				List<Task> cloudlets= broker.getCloudletList();
				
				HashMap<Integer,Double> vmPerformance = new HashMap<Integer,Double>();
				
				HashMap<Integer,List<Cloudlet>> vmCloudletMap = new HashMap<Integer,List<Cloudlet>>();
				for(Cloudlet cloudlet: cloudlets)
				{
					
					if (vmCloudletMap.get(cloudlet.getVmId())==null)
					{
						vmCloudletMap.put(cloudlet.getVmId(), new ArrayList<Cloudlet>());
					}
					vmCloudletMap.get(cloudlet.getVmId()).add(cloudlet);
					
					
					
				}
				
				
				
				int i=0;
				int destVmIndex=0;
				
				
				while(i<allocList.size())
				{
					if(vmDestroyedCount==broker.getTotalInstanceCountLimit()-broker.getPerQuantumInstanceCount())
						{CloudSim.resumeSimulation();return;}
					DynamicVm origVm1=((DynamicVm)VmList.getById(broker.getVmList(),allocList.get(i)));
					
					

					 int destVm= retainedVmList.get(destVmIndex++%retainedVmList.size());// the id of the new vm, you want to move the cloudlet to
					try{
					for(Cloudlet cloudlet: vmCloudletMap.get(origVm1.getId()))
					{
						cloudlet.setVmId(destVm);
						double timeRan=CloudSim.clock()-cloudlet.getSubmissionTime();
						//cloudlet.setCloudletLength((long) (cloudlet.getCloudletLength()-timeRan*origVm1.getMips()));
						int[] array = {cloudlet.getCloudletId(), origVm1.getUserId(), origVm1.getId(), destVm, broker.getVmsToDatacentersMap().get(origVm1.getId()),(int) cloudlet.getCloudletFinishedSoFar()};
						broker.sendNow(broker.getVmsToDatacentersMap().get(origVm1.getId()), CloudSimTags.CLOUDLET_MOVE, array);
					}
					}
					catch(Exception e)
					{
						
					}
					
				//	vmToVmMap.put(vmPerfList.get(i).getVmId(), vm.getId());
					i++;
					
					
				}
				
				
				int tempI= i;
				
				while(--i>=0)
				{
					DynamicVm origVm1=((DynamicVm)VmList.getById(broker.getVmList(),allocList.get(i)));
					
					
					Iterator<Vm> iter= broker.getVmsCreatedList().iterator();
					while(iter.hasNext())
					{
						if(iter.next().getId()==origVm1.getId())
							iter.remove();
					}
					allocList.remove(i);
					broker.sendNow(broker.getVmsToDatacentersMap().get(origVm1.getId()), CloudSimTags.VM_DESTROY,origVm1);
					vmDestroyedCount++;
					
				}
				if(vmDestroyedCount==broker.getTotalInstanceCountLimit()-broker.getPerQuantumInstanceCount())
					{CloudSim.resumeSimulation();return;}
				CloudSim.resumeSimulation();
				}
				CloudSim.resumeSimulation();
			}
			public GreedyDataCenterBroker getBroker() {
				return broker;
			}
			public void setBroker(GreedyDataCenterBroker_Migration broker) {
				this.broker = broker;
			}
			public List<Integer> getRetainedVmList() {
				return retainedVmList;
			}
			public void setRetainedVmList(List<Integer> retainedVmList) {
				this.retainedVmList = retainedVmList;
			}
			public List<Integer> getAllocList() {
				return allocList;
			}
			public void setAllocList(List<Integer> destroyedVmList) {
				this.allocList = destroyedVmList;
			}

		}


	

}
