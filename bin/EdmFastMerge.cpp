/*----------------------------------------------------------------------

This is a generic main that can be used with any plugin and a 
PSet script.   See notes in EventProcessor.cpp for details about
it.

$Id: EdmFastMerge.cpp,v 1.12 2006/09/21 19:41:56 wmtan Exp $

----------------------------------------------------------------------*/  

#include <exception>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <boost/program_options.hpp>
#include "IOPool/Common/bin/FastMerge.h"
#include "Cintex/Cintex.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/Utilities/interface/ProblemTracker.h"
#include "FWCore/Utilities/interface/Presence.h"
#include "FWCore/Utilities/interface/PresenceFactory.h"
#include "FWCore/ServiceRegistry/interface/ServiceRegistry.h"


using namespace boost::program_options;

// -----------------------------------------------

int main(int argc, char* argv[]) {

  std::string const kProgramName = argv[0];

  options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("in,i", value<std::vector<std::string> >(), "input files")
    ("out,o", value<std::string>(), "output file")
    ("logical,l", value<std::string>(), "logical name for output file")
    ("catalog,c", value<std::string>(), "input catalog")
    ("writecatalog,w", value<std::string>(), "output catalog")
    ("jobreport,j", value<std::string>(), "job report file")
    ("strict,s", "be strict about file merging")
    ("skip,k", "skip missing/unreadable input files");

  positional_options_description p;
  p.add("in", -1);

  variables_map vm;

  try
    {
      store(command_line_parser(argc, argv).options(desc).positional(p).run(), vm);
    }
  catch (boost::program_options::error const& x)
    {
      std::cerr << "Option parsing failure:\n"
		<< x.what() << '\n'
		<< "Try 'EdmFastMerge -h' for help.\n";
      return 1;
    }

  notify(vm);    

  if (vm.count("help")) {
    std::cerr << desc << "\n";
    return 1;
  }

  if (!vm.count("out")) {
    std::cerr << "output not set.\n";
    return 1;
  }

  if (!vm.count("in")) {
    std::cerr << "input not set.\n";
    return 1;
  }


  // Default is 'permissive' mode; be strict only if we're told to be.
  bool const beStrict = vm.count("strict");

  bool const skipMissing = vm.count("skip");

  std::vector<std::string> in = vm["in"].as<std::vector<std::string> >(); 

  std::string out = vm["out"].as<std::string>(); 

  std::string catalog = (vm.count("catalog") ? vm["catalog"].as<std::string>() : std::string()); 

  std::string outputCatalog = (vm.count("writecatalog") ? vm["writecatalog"].as<std::string>() : std::string()); 

  std::string lfn = (vm.count("logical") ? vm["logical"].as<std::string>() : std::string()); 

  int rc = 0;
  try {
    ROOT::Cintex::Cintex::Enable();

    // We must initialize the plug-in manager first
    edm::AssertHandler ah;

    // Load the message service plug-in
    boost::shared_ptr<edm::Presence> theMessageServicePresence;
    theMessageServicePresence = boost::shared_ptr<edm::Presence>(edm::PresenceFactory::get()->
      makePresence("MessageServicePresence").release());

    std::string config =
      "process EdmFastMerge = {"
	"service = MessageLogger {"
	  "untracked vstring destinations = {'cout','cerr'}"
	  "untracked PSet cout = {"
	    "untracked string threshold = 'INFO'"
	    "untracked PSet default = {untracked int32 limit = 10000000}"
	    "untracked PSet FwkJob = {untracked int32 limit = 0}"
	  "}"
	  "untracked PSet cerr = {"
	    "untracked string threshold = 'WARNING'"
	    "untracked PSet default = {untracked int32 limit = 10000000}"
	  "}";

    if (vm.count("jobreport")) {
      config += " untracked vstring fwkJobReports = {'";
      config += vm["jobreport"].as<std::string>(); 
      config += "'}";
    }

    config +=
	  "untracked vstring categories = {'FwkJob'}"
	  "untracked PSet FrameworkJobReport = {"
	    "untracked PSet default = {untracked int32 limit = 0}";

    config += (vm.count("jobreport") ?
	    "untracked PSet FwkJob = {untracked int32 limit = 10000000}" :
	    "untracked PSet FwkJob = {untracked int32 limit = 0}");

    config +=
	  "}"
	"}"
	"service = JobReportService{}"
	"service = SiteLocalConfigService{}"
	"service = AdaptorConfig{}"
      "}";

    //create the services
    edm::ServiceToken tempToken = edm::ServiceRegistry::createServicesFromConfig(config);

    //make the services available
    edm::ServiceRegistry::Operate operate(tempToken);

    edm::FastMerge(in, out, catalog, outputCatalog, lfn, beStrict, skipMissing);
  }
  catch (cms::Exception& e) {
    std::cout << "cms::Exception caught in "
	      << kProgramName
	      << '\n'
	      << e.explainSelf();
    rc = 1;
  }
  catch (seal::Error& e) {
    std::cout << "Exception caught in "
	      << kProgramName
	      << '\n'
	      << e.explainSelf();
    rc = 1;
  }
  catch (std::exception& e) {
    std::cout << "Standard library exception caught in "
	      << kProgramName
	      << '\n'
	      << e.what();
    rc = 1;
  }
  catch (...) {
    std::cout << "Unknown exception caught in "
	      << kProgramName;
    rc = 2;
  }

  return rc;
}
