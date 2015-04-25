package org.cloudbus.cloudsim;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;

import de.huberlin.wbi.dcs.DynamicHost;
import de.huberlin.wbi.dcs.workflow.scheduler.AbstractWorkflowScheduler;
import de.huberlin.wbi.dcs.workflow.scheduler.C2O;
import de.huberlin.wbi.dcs.workflow.scheduler.GreedyQueueScheduler;

public class GreedyDataCenterBroker extends C2O {

	private VmAllocationPolicy vmPolicy;
	private int perQuantumInstanceCount;
	private int totalInstanceCountLimit;
	private int numInstancesLaunched;
	private int numRunningInstances=0;
	private List<DynamicHost> allocatedHostList= new ArrayList<DynamicHost>();
	private Map<Integer, Integer> vmHostMap= new HashMap<Integer,Integer>();;
	public GreedyDataCenterBroker(String name,VmAllocationPolicy policy,int totalLimit,int perQLimit,int parameter1,int parameter2) throws Exception {
		super(name,parameter1,parameter2);
		vmPolicy=policy;
		totalInstanceCountLimit=totalLimit;
		perQuantumInstanceCount=perQLimit;
		
	}
	class AllocatedVM
	{
		int id;
		double availableMips;
		public double getAvailableMips() {
			return availableMips;
		}
		public void setAvailableMips(double availableMips) {
			this.availableMips = availableMips;
		}
		public AllocatedVM(int vmId, double availableMips) {
			super();
			this.id = vmId;
			this.availableMips = availableMips;
		}
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
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
			
			numRunningInstances++;
			DynamicHost host=(DynamicHost) vmPolicy.getVmTable().get("3-"+vmId);
			getVmsToDatacentersMap().put(vmId, datacenterId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in Datacenter #" + datacenterId + ", Host #"
					+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId());
			if(numRunningInstances==totalInstanceCountLimit)
			{
				vmHostMap.put(vmId,host.getId());
				Iterator<Entry<Integer,Integer>> iter= vmHostMap.entrySet().iterator();
				while(iter.hasNext())
				{
					Entry<Integer,Integer> entry=iter.next();
					allocList.add(new AllocatedVM(entry.getKey(),((DynamicHost) vmPolicy.getVmTable().get("3-"+entry.getKey())).getMipsPerPe()/((DynamicHost) vmPolicy.getVmTable().get("3-"+entry.getKey())).getNumberOfCusPerPe()));
				}
				Collections.sort(allocList, new Comparator<AllocatedVM>(){
					   public int compare(AllocatedVM o1, AllocatedVM o2){
					      return (int) (o1.getAvailableMips() - o2.getAvailableMips());
					   }
					});
				
				for(int i=0;i<allocList.size()-perQuantumInstanceCount;i++)
				{
					AllocatedVM vm=allocList.get(i);
					Log.printLine(CloudSim.clock() + ": " + getName() + ": Destroying VM #" + vm.getId());
					Vm machine=null;
					for(Vm tvm:getVmsCreatedList())
					{
						if(tvm.getId()==vm.getId())
						{machine=tvm;
							break;
						}
						
						
					}
					if(machine==null)
						System.out.println("incorrect destruction");
					else
					{getVmsCreatedList().remove(machine);
						sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY,machine);
					}
					
				}
				
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
			submitCloudlets();
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
	
	

	public Map<Integer, Integer> getVmHostMap() {
		return vmHostMap;
	}



	public void setVmHostMap(Map<Integer, Integer> vmHostMap) {
		this.vmHostMap = vmHostMap;
	}



	public VmAllocationPolicy getVmPolicy() {
		return vmPolicy;
	}

	public void setVmPolicy(VmAllocationPolicy vmPolicy) {
		this.vmPolicy = vmPolicy;
	}

	public int getPerQuantumInstanceCount() {
		return perQuantumInstanceCount;
	}

	public void setPerQuantumInstanceCount(int perQuantumInstanceCount) {
		this.perQuantumInstanceCount = perQuantumInstanceCount;
	}

	

	public int getTotalInstanceCountLimit() {
		return totalInstanceCountLimit;
	}

	public void setTotalInstanceCountLimit(int totalInstanceCountLimit) {
		this.totalInstanceCountLimit = totalInstanceCountLimit;
	}

	public int getNumInstancesLaunched() {
		return numInstancesLaunched;
	}

	public void setNumInstancesLaunched(int numInstancesLaunched) {
		this.numInstancesLaunched = numInstancesLaunched;
	}

	public int getNumRunningInstances() {
		return numRunningInstances;
	}

	public void setNumRunningInstances(int numRunningInstances) {
		this.numRunningInstances = numRunningInstances;
	}

}
