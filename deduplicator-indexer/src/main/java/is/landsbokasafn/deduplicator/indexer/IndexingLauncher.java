/* Copyright (C) 2006-2014 National and University Library of Iceland (NULI)
 * 
 * This file is part of the DeDuplicator (Heritrix add-on module).
 * 
 *  NULI licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package is.landsbokasafn.deduplicator.indexer;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.cli.Option;
import org.apache.log4j.PropertyConfigurator;
import org.archive.util.DateUtils;

/**
 * This class handles loading configuration files, parsing command line arguments, loading the crawl data iterator
 * and any other classes needed before starting the indexing process. 
 */
public class IndexingLauncher {
	
	private static final String INDEX_URL_KEY = "deduplicator.indexurl";
	private static final String CANONICAL_CONF_KEY = "deduplicator.canonicalurl";
	private static final String ETAG_CONF_KEY = "deduplicator.etag";
	private static final String MIME_CONF_KEY = "deduplicator.mime";
	private static final String WHITELIST_CONF_KEY = "deduplicator.whitelist";
	private static final String ADD_TO_INDEX_CONF_KEY = "deduplicator.add";
	private static final String ITERATOR_CONF_KEY = "deduplicator.crawldataiterator";
	private static final String VERBOSE_CONF_KEY = "deduplicator.verbose";
	
	private static void loadConfiguration() {
		// Load properties file, either from heritrix.home/conf or
		// path specified via -Ddeduplicator.config JVM option
		String configFilename = System.getProperty("deduplicator.config");
		if (configFilename == null || configFilename.isEmpty()) {
			// Configuration file not explicitly set. Use default.
			// This works if invoked via the provided script. Will fail otherwise unless -Ddeduplicator.home is set
			configFilename = System.getProperty("deduplicator.home") + File.separator + "conf" + File.separator 
				+ "deduplicator.properties";							
		}
		File configFile = new File(configFilename);
		if (configFile.exists() == false ) {
			System.out.println("Unable to find configuration file " + configFilename);
			System.exit(1);
		}

		// Load log4j config, assumes same path as config file
		String log4jconfig = configFile.getParent() + File.separator + "deduplicator-log4j.properties";
		PropertyConfigurator.configure(log4jconfig);
		
		// Copy properties from config file to System properties
		try {
			System.getProperties().load(new FileReader(configFile));
		} catch (IOException e) {
			System.err.println("Unable to read configuration file");
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private static boolean readBooleanConfig(String propertyName, boolean fallback) {
		String prop = System.getProperty(propertyName);
		if (prop==null) {
			return fallback;
		}
		return prop.equalsIgnoreCase("true");
	}
	
	private static String readStringConfig(String propertyName, String fallback) {
		String prop = System.getProperty(propertyName);
		if (prop==null) {
			return fallback;
		}
		return prop;
	}
	
	public static void main(String[] args) throws Exception {
    	loadConfiguration();

        // Set default values for all settings
    	boolean verbose = readBooleanConfig(VERBOSE_CONF_KEY, true);
        boolean etag = readBooleanConfig(ETAG_CONF_KEY, false);
        boolean canonical = readBooleanConfig(CANONICAL_CONF_KEY, true);
        boolean indexURL = readBooleanConfig(INDEX_URL_KEY, true);
        boolean addToIndex = readBooleanConfig(ADD_TO_INDEX_CONF_KEY, false);
        String mimefilter = readStringConfig(MIME_CONF_KEY, "^text/.*");
        boolean whitelist = readBooleanConfig(WHITELIST_CONF_KEY, false);
        String iteratorClassName = readStringConfig(ITERATOR_CONF_KEY, WarcIterator.class.getName());
    	
		// Parse command line options    	
        CommandLineParser clp = new CommandLineParser(args,new PrintWriter(System.out));
        Option[] opts = clp.getCommandLineOptions();
        for(int i=0 ; i<opts.length ; i++){
            Option opt = opts[i];
            switch(opt.getId()){
            case 'w' : whitelist=true; break;
            case 'a' : addToIndex=true; break;
            case 'e' : etag=true; break;
            case 'h' : clp.usage(0); break;
            case 'i' : iteratorClassName = opt.getValue(); break;
            case 'm' : mimefilter = opt.getValue(); break;
            case 'u' : indexURL = false; break;
            case 's' : canonical = false; break;
            case 'v' : verbose = true; break;
            }
        }
        
        if (!indexURL && canonical) {
        	canonical=false;
        }

        List<String> cargs = clp.getCommandLineArguments(); 
        if(cargs.size() != 2){
            // Should be exactly two arguments. Source and target!
            clp.usage(0);
        }
        
        String source = cargs.get(0);
        String target = cargs.get(1);

        // Load the CrawlDataIterator
        CrawlDataIterator iterator = (CrawlDataIterator)Class.forName(iteratorClassName).newInstance();

        // Print initial stuff
        System.out.println("Indexing: " + source);
        System.out.println(" - Index URL: " + indexURL);
        System.out.println(" - Mime filter: " + mimefilter + 
                " (" + (whitelist?"whitelist":"blacklist")+")");
        System.out.println(" - Includes" + 
                (canonical?" <canonical URL>":"") +
                (etag?" <etag>":""));
        System.out.println(" - Iterator: " + iteratorClassName);
        System.out.println("   - " + iterator.getSourceType());
        System.out.println("Target: " + target);
        if(addToIndex){
            System.out.println(" - Add to existing index (if any)");
        } else {
            System.out.println(" - New index (erases any existing index at " +
                    "that location)");
        }
        
        iterator.initialize(source);

        // Create the index
        long start = System.currentTimeMillis();
        IndexBuilder di = new IndexBuilder(
        		target,
        		indexURL,
                canonical, 
                etag,
                addToIndex);
        di.writeToIndex(iterator, mimefilter, !whitelist, verbose);
        
        // Clean-up
        di.close();
        
        System.out.println("Total run time: " + 
        		DateUtils.formatMillisecondsToConventional(System.currentTimeMillis()-start));
    }
}
