package jlogstash;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Test {
	
	public static void main(String[] args) throws InterruptedException{
//		final ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
//		Thread t = new Thread(new Runnable(){
//            long ss = System.currentTimeMillis();
//			@Override
//			public void run() {
//				// TODO Auto-generated method stub
//				while(true){
//					try {
//						String gg = queue.poll();
//						System.out.println(gg);
//						if("1000000".equals(gg)){
//							System.out.println(System.currentTimeMillis()-ss);
//							break;
//						}
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
//				}	
//			}});
//		t.start();		
//		for(int i=0;i<=1000000;i++){
//			queue.offer(i+"");
//		}
//		Thread.sleep(3600000);
		long ss = 3324271410l;
		ss = ss - 0l;
		System.out.println(ss);
	}
}
