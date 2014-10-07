package is.landsbokasafn.deduplicator.heritrix;

import java.io.PrintWriter;

import org.archive.crawler.reporting.Report;
import org.archive.crawler.reporting.StatisticsTracker;

public class DeDuplicatorReport extends Report {
	
	DeDuplicator deDuplicator;
	public DeDuplicator getDeDuplicator() {
		return deDuplicator;
	}
	public void setDeDuplicator(DeDuplicator deDuplicator) {
		this.deDuplicator = deDuplicator;
	}

	@Override
	public void write(PrintWriter writer, StatisticsTracker stats) {
		writer.write(deDuplicator.report());
	}

	@Override
	public String getFilename() {
		return deDuplicator.getBeanName()+"-report.txt";
	}

}
