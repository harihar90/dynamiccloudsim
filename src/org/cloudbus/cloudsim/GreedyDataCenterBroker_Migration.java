package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

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
			DynamicHost host=(DynamicHost) getVmPolicy().getVmTable().get("3-"+vmId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			getVmsToDatacentersMap().put(vmId, datacenterId);
			
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in Datacenter #" + datacenterId + ", Host #"
					+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId()+" with mips:"+VmList.getById(getVmsCreatedList(), vmId).getHost().getTotalMips()/VmList.getById(getVmsCreatedList(), vmId).getHost().getNumberOfPes());
			if(getNumRunningInstances()==getPerQuantumInstanceCount())
			{
				
				if(!monitorStarted){
				monitorStarted=true;
				getVmHostMap().put(vmId,host.getId());
				
				Timer t = new Timer();

				

				Thread monitor=new Thread(new PerformanceMonitoringTask(this, threshold));
				monitor.setPriority(Thread.MAX_PRIORITY);
				monitor.start();
				
				
				Thread.sleep(1000);
				}
			}
			else{
				
			 
			getVmHostMap().put(vmId,host.getId());
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
			submitCloudlets();
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
			public Double getMeanMips() {
			return meanMips;
		}
		public void setMeanMips(Double meanMips) {
			this.meanMips = meanMips;
		}
		HashMap<Integer,Integer> cloudletCountMap = new HashMap<Integer,Integer>();
			public PerformanceMonitoringTask(GreedyDataCenterBroker_Migration broker,Double meanMips) {
				this.setBroker(broker);
				this.setMeanMips(meanMips);
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
				while(j++ !=0){
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
				List<VmPerformanceData> vmPerfList=new ArrayList<VmPerformanceData>();
				
				for(Integer vmId:vmCloudletMap.keySet())
				{
					DynamicHost dynamicHost = (DynamicHost) broker.getVmPolicy().getVmTable().get("3-"+vmId);
					if(dynamicHost==null || broker.getVmsToDatacentersMap().get(vmId)==null || CloudSim.clock()-((DynamicVm)VmList.getById(broker.getVmList(),vmId)).getStartTime()>Parameters.DELTA)
						continue;
					double performance=(dynamicHost.getMipsPerPe()/dynamicHost.getNumberOfCusPerPe());
					VmPerformanceData perf= new VmPerformanceData();
					perf.setVmId(vmId);
					perf.setCloudletsLength(performance);
					vmPerfList.add(perf);
				}
				
				Collections.sort(vmPerfList, new Comparator<VmPerformanceData>(){
					   public int compare(VmPerformanceData o1, VmPerformanceData o2){
					      return (int) (o1.getCloudletsLength() - o2.getCloudletsLength());
					   }
					});
				int i=0;
				List<DynamicVm> submittedVmList= new ArrayList<DynamicVm>();
				Map<Integer,Integer> vmToVmMap = new HashMap<Integer,Integer>();
				
				while(i<vmPerfList.size() && vmPerfList.get(i).getCloudletsLength()<broker.getThreshold())
				{
					if(vmDestroyedCount==broker.getTotalInstanceCountLimit()-broker.getPerQuantumInstanceCount())
						break;
					DynamicVm origVm1=((DynamicVm)VmList.getById(broker.getVmList(),vmPerfList.get(i).getVmId()));
					int vmDestId=vmToVmMap.get(origVm1.getId()); // the id of the new vm, you want to move the cloudlet to
					
					for(Cloudlet cloudlet: vmCloudletMap.get((vmPerfList).get(i).getVmId()))
					{
						cloudlet.setVmId(vmDestId);
						double timeRan=CloudSim.clock()-cloudlet.getSubmissionTime();
						cloudlet.setCloudletLength((long) (cloudlet.getCloudletLength()-timeRan*origVm1.getMips()));
						int[] array = {cloudlet.getCloudletId(), origVm1.getUserId(), origVm1.getId(), vmDestId, broker.getVmsToDatacentersMap().get(origVm1.getId()),(int) cloudlet.getCloudletFinishedSoFar()};
						broker.sendNow(broker.getVmsToDatacentersMap().get(origVm1.getId()), CloudSimTags.CLOUDLET_MOVE, array);
					}
					
					
				//	vmToVmMap.put(vmPerfList.get(i).getVmId(), vm.getId());
					i++;
					
					
				}
				
				
				int tempI= i;
				while(--i>=0)
				{
					
					
				}
				i=tempI;
				while(--i>=0)
				{
					DynamicVm origVm1=((DynamicVm)VmList.getById(broker.getVmList(),vmPerfList.get(i).getVmId()));
					Iterator<Vm> iter= broker.getVmsCreatedList().iterator();
					while(iter.hasNext())
					{
						if(iter.next().getId()==origVm1.getId())
							iter.remove();
					}
					broker.sendNow(broker.getVmsToDatacentersMap().get(origVm1.getId()), CloudSimTags.VM_DESTROY,origVm1);
					
				}
				if(vmDestroyedCount==broker.getTotalInstanceCountLimit())
					break;
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

		}


	

}
