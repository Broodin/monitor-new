package symc.monitor;

import java.util.Timer;
import java.util.TimerTask;

public class Main {

	public static void main(String[] args) {
		/* Enter your code here. Read input from STDIN. Print output to STDOUT. Your class should be named Solution. */
		TimerTask timerTask = new MonitorRunner(args);
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(timerTask, 0, 60000);
		
        System.out.println("TimerTask started");
		
	}
}
