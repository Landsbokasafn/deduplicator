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

import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.cli.UnrecognizedOptionException;


/**
 * Print DigestIndexer command-line usage message.
 *
 * @author Kristinn Sigur&eth;sson
 */
public class CommandLineParser {
    private static final String USAGE = "Usage: ";
    private static final String NAME = "DigestIndexer";
    private Options options = null;
    private CommandLine commandLine = null;
    private PrintWriter out = null;

    /**
     * Block default construction.
     *
     */
    @SuppressWarnings("unused")
	private CommandLineParser() {
        super();
    }

    /**
     * Constructor.
     *
     * @param args Command-line arguments to process.
     * @param out PrintStream to write on.
     *
     * @throws ParseException Failed parse of command line.
     */
    public CommandLineParser(String [] args, PrintWriter out)
    throws ParseException {
        super();

        this.out = out;

        this.options = new Options();
        this.options.addOption(new Option("h","help", false,
                "Prints this message and exits."));
        
        Option opt = new Option("u","no-url-index", false,
                "Do not index the URLs. Index will only be searchable by digest.");
        this.options.addOption(opt);
        
        this.options.addOption(new Option("s","no-canonicalized", false,
                "Do not add a canonicalized version of the URL to the index."));
        
        this.options.addOption(new Option("e","etag", false,
        		"Include etags in the index (if available in the source)."));

        opt = new Option("m","mime", true,
                "A filter on what mime types are added into the index " +
                "(blacklist). Default: ^text/.*");
        opt.setArgName("reg.expr.");
        this.options.addOption(opt);

        this.options.addOption(new Option("w","whitelist", false,
                "Make the --mime filter a whitelist instead of blacklist."));
        
        this.options.addOption(new Option("v","verbose", false,
                "Make the program print progress info to standard out."));
        
        opt = new Option("i","iterator", true,
                "An iterator suitable for the source data (default iterator " +
                "works WARC files).");
        opt.setArgName("classname");
        this.options.addOption(opt);

        this.options.addOption(new Option("a","add", false,
            "Add source data to existing index."));

        PosixParser parser = new PosixParser();
        try {
            this.commandLine = parser.parse(this.options, args, false);
        } catch (UnrecognizedOptionException e) {
            usage(e.getMessage(), 1);
        }
    }

    /**
     * Print usage then exit.
     */
    public void usage() {
        usage(0);
    }

    /**
     * Print usage then exit.
     *
     * @param exitCode The exit code to return 
     */
    public void usage(int exitCode) {
        usage(null, exitCode);
    }

    /**
     * Print message then usage then exit.
     *
     * The JVM exits inside in this method.
     *
     * @param message Message to print before we do usage.
     * @param exitCode Exit code to use in call to System.exit.
     */
    public void usage(String message, int exitCode) {
        outputAndExit(message, true, exitCode);
    }

    /**
     * Print message and then exit.
     *
     * The JVM exits inside in this method.
     *
     * @param message Message to print before we do usage.
     * @param exitCode Exit code to use in call to System.exit.
     */
    public void message(String message, int exitCode) {
        outputAndExit(message, false, exitCode);
    }

    /**
     * Print out optional message an optional usage and then exit.
     *
     * Private utility method.  JVM exits from inside in this method.
     *
     * @param message Message to print before we do usage.
     * @param doUsage True if we are to print out the usage message.
     * @param exitCode Exit code to use in call to System.exit.
     */
    private void outputAndExit(String message, boolean doUsage, int exitCode) {
        if (message !=  null) {
            this.out.println(message);
        }

        if (doUsage) {
            HelpFormatter formatter =
                new DigestHelpFormatter();
            formatter.printHelp(this.out, 80, NAME, "Options:", this.options,
                1, 2, "Arguments:", false);
            this.out.println(" source                     Data to iterate over (typically a directory containing");
            this.out.println("                            WARC files). If using a non-standard iterator, consult");
            this.out.println("                            relevant documentation");
            this.out.println(" target                     Target directory for index output. Directory need not");
            this.out.println("                            exist, but unless --add should be empty.");
        }

        // Close printwriter so stream gets flushed.
        this.out.close();
        System.exit(exitCode);
    }

    /**
     * @return Options passed on the command line.
     */
    public Option [] getCommandLineOptions() {
        return this.commandLine.getOptions();
    }

    /**
     * @return Arguments passed on the command line.
     */
    @SuppressWarnings({ "unchecked" })
	public List<String> getCommandLineArguments() {
        return this.commandLine.getArgList();
    }

    /**
     * @return Command line.
     */
    public CommandLine getCommandLine() {
        return this.commandLine;
    }

    
    /**
     * Override so can customize usage output.
     */
    public class DigestHelpFormatter extends HelpFormatter {
        public DigestHelpFormatter() {
            super();
        }

        public void printUsage(PrintWriter pw, int width, String cmdLineSyntax) {
            out.println(USAGE + NAME + " --help");
            out.println(USAGE + NAME + " [options] source target");
        }

        public void printUsage(PrintWriter pw, int width,
            String app, Options options) {
            this.printUsage(pw, width, app);
        }
    }
}
