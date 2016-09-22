package symc.monitor;

public class ClusterData {
	private String queue_name;
	private float max_capacity;
	private float used_capacity;
	private long date;
	
	public void setQueueName(String queue_name){
		this.queue_name = queue_name;
	}
	
	public String getQueueName(){
		return this.queue_name;
	}
	
	public void setMaxCapacity(float max_capacity){
		this.max_capacity = max_capacity;
	}
	
	public float getMaxCapacity(){
		return this.max_capacity;
	}
	
	public void setUsedCapacity(float used_capacity){
		this.used_capacity = used_capacity;
	}
	
	public float getUsedCapacity(){
		return this.used_capacity;
	}
	
	public void setDate(long date){
		this.date= date;
	}
	
	public long getdate(){
		return this.date;
	}
	
	public String toString(){
		return queue_name + "\n" + used_capacity;
		
	}
}
