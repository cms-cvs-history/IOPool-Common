/*----------------------------------------------------------------------

This is a generic main that can be used with any plugin and a 
PSet script.   See notes in EventProcessor.cpp for details about
it.

$Id: EdmFastMerge.cpp,v 1.2 2006/02/28 19:29:57 wmtan Exp $

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


// -----------------------------------------------

int main(int argc, char* argv[]) {

  std::string const kProgramName = argv[0];

  boost::program_options::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("in,i", boost::program_options::value<std::vector<std::string> >(), "input files")
    ("out,o", boost::program_options::value<std::string>(), "output file");

  boost::program_options::positional_options_description p;
  p.add("in", -1);

  boost::program_options::variables_map vm;

  boost::program_options::store(boost::program_options::command_line_parser(argc, argv).
          options(desc).positional(p).run(), vm);

  // boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);    

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 1;
  }

  if (!vm.count("out")) {
    std::cout << "output not set.\n";
    return 1;
  }

  if (!vm.count("in")) {
    std::cout << "input not set.\n";
    return 1;
  }

  std::vector<std::string> in = vm["in"].as<std::vector<std::string> >(); 

  std::string out = vm["out"].as<std::string>(); 

  int rc = 0;
  try {
    ROOT::Cintex::Cintex::Enable();
    edm::FastMerge(in, out);
  }
  catch (seal::Error& e) {
    std::cout << "Exception caught in "
                                << kProgramName
                                << "\n"
                                << e.explainSelf();
    rc = 1;
  }
  catch (std::exception& e) {
    std::cout << "Standard library exception caught in "
                                << kProgramName
                                << "\n"
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
