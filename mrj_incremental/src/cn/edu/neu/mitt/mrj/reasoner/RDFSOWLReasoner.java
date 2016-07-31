package cn.edu.neu.mitt.mrj.reasoner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.neu.mitt.mrj.reasoner.owl.OWLReasoner;
import cn.edu.neu.mitt.mrj.reasoner.rdfs.RDFSReasoner;

public class RDFSOWLReasoner {
	
	protected static Logger log = LoggerFactory.getLogger(RDFSOWLReasoner.class);
	
	static int step = 0;
	
	private static void parseArgs(String[] args) {
		
		for(int i=0;i<args.length; ++i) {
			if (args[i].equalsIgnoreCase("--startingStep")) {
				RDFSOWLReasoner.step = Integer.valueOf(args[++i]);
			}

		}
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.out.println("USAGE: RDFSOWLReasoner [pool path] [options]");
			return;
		}
		
		parseArgs(args);
		

		try {
			long totalDerivation = 0;
			boolean continueDerivation = true;
			long startTime = System.currentTimeMillis();
			long rdfsDerivation = 0;
			long owlDerivation = 0;
			boolean firstLoop = true;
			
			while (continueDerivation) {
				//Do RDFS reasoning
				/*
				 * Modified  2015/7/1
				 * Move rdfsReasoner owlReasoner inside the loop
				 */
				
				RDFSReasoner rdfsReasoner = new RDFSReasoner();
				OWLReasoner owlReasoner = new OWLReasoner();
				
				if (owlDerivation == 0 && !firstLoop) {
					rdfsDerivation = 0;
				} else {
					RDFSReasoner.step = RDFSOWLReasoner.step;
					rdfsDerivation = rdfsReasoner.launchDerivation(args);
				}
				
				//Do OWL reasoning
				if (rdfsDerivation == 0 && !firstLoop) {
					owlDerivation = 0;
				} else {
					OWLReasoner.step = RDFSReasoner.step;
					owlDerivation = owlReasoner.launchClosure(args);
					RDFSOWLReasoner.step = OWLReasoner.step;
				}
				totalDerivation += owlDerivation + rdfsDerivation;
				continueDerivation = (owlDerivation + rdfsDerivation) > 0;
				firstLoop = false;
			}
//			log.info("Number triples derived: " + totalDerivation);
//			log.info("Time derivation: " + (System.currentTimeMillis() - startTime));
			System.out.println("Number triples derived: " + totalDerivation);
			System.out.println("Time derivation: " + (System.currentTimeMillis() - startTime));
		} catch (Exception e) {
			log.error(e.getMessage());
			e.printStackTrace();
		}
	}
	
}