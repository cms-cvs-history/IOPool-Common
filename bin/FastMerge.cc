#include "IOPool/Common/bin/FastMerge.h"

#include <memory>

#include "TChain.h"
#include "TChainElement.h"
#include "TError.h"
#include "TFile.h"
#include "TTree.h"
#include "TObjArray.h"
#include "TBranch.h"

//#define VERBOSE

#include "DataFormats/Provenance/interface/ProductRegistry.h"
#include "DataFormats/Provenance/interface/ParameterSetBlob.h"
#include "DataFormats/Provenance/interface/ProcessHistory.h"
#include "DataFormats/Provenance/interface/FileFormatVersion.h"
#include "DataFormats/Provenance/interface/BranchType.h"
#include "DataFormats/Provenance/interface/LuminosityBlockAuxiliary.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Catalog/interface/FileCatalog.h"
#include "FWCore/Catalog/interface/InputFileCatalog.h"
#include "FWCore/Utilities/interface/GetFileFormatVersion.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/MessageLogger/interface/JobReport.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "POOLCore/Guid.h"

// Notes:
//
// The exact meaning of 'strict mode' and 'permissive mode' depend on the file format version.
//
// Version 0 files are not handled by this version of the program;
// one must use the FastMerge (in EdmFastMerge) from the older release
// to merge files from the older release.
//
// In the current version, the merging attempt stops when the first
// file that is not compatible is encountered.

namespace edm {
  namespace {
    //----------------------------------------------------------------
    //
    // Utility functions

    // Functions to make up for Root design flaw: it can create
    // objects that are in an unusable (called by Root 'zombie')
    // state, rather than having constructors throw exceptions.

    // Create a TFile in by opening the given file,
    // or throw a suitable exception (unless noThrow is true).
    // The file is opened in read mode by default,
    // but opening in recreate mode can be requested
    std::auto_ptr<TFile>
    openTFile(std::string const& filename, std::string const& logicalFileName,
	bool const openInWriteMode = false,
	bool const noThrow = false) {
      char const* const option = openInWriteMode ? "recreate" : "read";
      if (noThrow) gErrorIgnoreLevel = kBreak;
      std::auto_ptr<TFile> result(TFile::Open(filename.c_str(),option));
      if (noThrow) gErrorIgnoreLevel = kError;
      if (!result.get() || result->IsZombie()) {
	if (noThrow) return std::auto_ptr<TFile>();
        if (filename.empty()) {
          throw cms::Exception("LogicalFileNameNotFound", "FileCatalog::findFile()\n")
            << "Logical file name " << logicalFileName << " was not found in the file catalog.\n"
            << "If you wanted a local file, you forgot the 'file:' prefix\n"
            << "before the file name in your configuration file.\n";
        }
	throw cms::Exception("RootFailure")
	  << "Unable to open file: " 
	  << filename
	  << '\n';
      }
      return result;
    }

    // Get a TTree of the given name from the already-open TFile, or
    // throw a suitable exception.
    TTree* 
    getTTreeOrThrow(TFile& file, char const* treename) {
      TTree* result = dynamic_cast<TTree*>(file.Get(treename));
      if (!result)
	throw cms::Exception("RootFailure")
	  << "Unable to find the TTree: "
	  << treename
	  << "\n in TFile: "
	  << file.GetName()
	  << '\n';
      if (result->IsZombie())
	throw cms::Exception("RootFailure")
	  << "'TFile::Get' for TTree: " << treename
	  << "\nin file: " << file.GetName()
	  << "\nreturned a 'zombie' tree\n";
      return result;
    }

    TTree*
    getTTreeOrThrow(TFile& file, std::string const& treename) {
      return getTTreeOrThrow(file, treename.c_str());
    }


    // Get a TTree of the given name from the already-open TFile, or
    // return a null pointer. Make sure not to return a non-null
    // pointer to a 'zombie' TTree.
    TTree*
    getTTree(TFile& file, char const* treename) {
      TTree* result = dynamic_cast<TTree*>(file.Get(treename));
      return (result && !result->IsZombie())
	? result
	: 0;
    }

    // Create a TChain with the given name, or throw a suitable
    // exception.  However, if tree is not present, just return a null pointer.
    std::auto_ptr<TChain>
    makeTChainOrThrow(std::string const& name, TFile & curfile) {
      if (dynamic_cast<TTree*>(curfile.Get(name.c_str())) == 0) return std::auto_ptr<TChain>();
      std::auto_ptr<TChain> result(new TChain(name.c_str()));
      if (result->IsZombie())
	throw cms::Exception("RootFailure")
	  << "Unable to create a TChain with name: "
	  << name
	  << '\n';
      return result;
    }

    // Try to add the given filename to the given TChain. If this
    // fails, throw an appropriate exception.
    void 
    addFilenameToTChain(TChain& chain, std::string const& filename) {
//    if (chain.AddFile(filename.c_str()) == 0)
      if (chain.AddFile(filename.c_str(),-1) == 0)
	throw cms::Exception("RootFailure")
	  << "TChain::AddFile failed to add the file: "
	  << filename
	  << "\nto the TChain for TTree: "
	  << chain.GetName()
	  << '\n';
    }

    void
    mergeTChain(TChain & chain, TFile & outfile) {
      Int_t const     basketsize(32000);
      // We have to specify 'keep' to prevent ROOT from calling delete
      // on the TFile* we pass to TChain::Merge; we specify 'fast' to
      // get ROOT to transfer raw data, rather than unzipping and
      // re-creating objects.
      Option_t const* opts("fast,keep");

      if (chain.Merge(&outfile, basketsize, opts) == 0) {
        throw cms::Exception("RootFailure")
          << "TChain::Merge failed to merge"
          << "\nto the TChain for TTree: "
          << chain.GetName()
          << '\n';
      }
    }

    // Helper functions for the comparison of files.
    void
    compare_char(TTree* lh, TTree* rh, 
		 TBranch* pb1, TBranch* pb2, 
		 int nEntries, std::string const& fileName) {
      char pr1[1024];
      char pr2[1024];
      memset(pr1, sizeof(pr1), '\0');
      memset(pr2, sizeof(pr2), '\0');
      pb1->SetAddress(pr1);
      pb2->SetAddress(pr2);
      for (int j = 0; j < nEntries; ++j) {
        pb1->GetEntry(j);
        pb2->GetEntry(j);
        if (strcmp(pr1, pr2)) {
          throw cms::Exception("MismatchedInput","FastMerge::compare_char()")
            << "File " << fileName << "\nhas different " << rh->GetName() << " tree than previous files\n";
        }
      }
    }

    void
    compare(TTree* lh, TTree* rh,  
	    std::string const& fileName) {
      int nEntries = lh->GetEntries();
      if (nEntries != rh->GetEntries()) {
	  throw cms::Exception("MismatchedInput")
	    << "Number of entries in TTree: "
	    << lh->GetName()
	    << "\nfrom file: "
	    << fileName
	    << "\ndoes not match that from the original file\n";
      }
      int nBranches = lh->GetNbranches();
      if (nBranches != rh->GetNbranches()) {
	  throw cms::Exception("MismatchedInput")
	    << "Number of branches in TTree: "
	    << lh->GetName()
	    << "\nfrom file: "
	    << fileName
	    << "\ndoes not match that from the original file\n";
      }
      TObjArray* ta1 = lh->GetListOfBranches();
      TObjArray* ta2 = rh->GetListOfBranches();
      for (int i = 0; i < nBranches; ++i) {
	  TBranch* pb1 = static_cast<TBranch*>(ta1->At(i));
	  TBranch* pb2 = static_cast<TBranch*>(ta2->At(i));
	  if (*pb1->GetName() != *pb2->GetName()) {
	      throw cms::Exception("MismatchedInput")
		<< "Names of branches in TTree: "
		<< lh->GetName()
		<< "\nfrom file: "
		<< fileName
		<< "\ndoes not match that from the original file\n";
	  }
	  if (*pb1->GetTitle() != *pb2->GetTitle()) {
	      throw cms::Exception("MismatchedInput")
		<< "Titles of branches in TTree: "
		<< lh->GetName()
		<< "\nfrom file: "
		<< fileName
		<< "\ndoes not match that from the original file\n";
	      return;
	  }
	  compare_char(lh, rh, pb1, pb2, nEntries, fileName);
      }
    }
    
    void
    getBranchNamesFromRegistry(ProductRegistry& reg,
			       std::vector<std::string>& names) {
      typedef ProductRegistry::ProductList prodlist;
      typedef prodlist::const_iterator iter;

      prodlist const& prods = reg.productList();
      for (iter i=prods.begin(), e=prods.end(); i!=e; ++i) {
        if (i->second.branchType() == InEvent) {
	  i->second.init();
	  names.push_back(i->second.branchName());
        }
      }
    }

    void
    checkStrictMergeCriteria(ProductRegistry& reg, 
			     int fileFormatVersion,
			     std::string const& filename,
			     BranchDescription::MatchMode matchMode) {
     

      // This is suitable only for file format version 1.
      if (fileFormatVersion < 1)
	throw cms::Exception("MismatchedInput")
	  << "This version of checkStrictMergeCriteria"
	  << " only supports file version 1 or greater\n";

      if (matchMode == BranchDescription::Permissive) return;

      // We require exactly one 'ProcessConfigurationID' and one
      // 'ParameterSetID' for each branch in the file.
      typedef ProductRegistry::ProductList::const_iterator iter;
      ProductRegistry::ProductList const& prods = reg.productList();
      for (iter i=prods.begin(),e=prods.end(); i!=e; ++i) {
	  if (i->second.processConfigurationIDs().size() != 1)
	    throw cms::Exception("MismatchedInput")
	      << "File " << filename
	      << "\nhas " << i->second.processConfigurationIDs().size()
	      << " ProcessConfigurations"
	      << "\nfor branch " << i->first
	      << "\nand only one is allowed for strict merge\n";

	  if (! i->second.isPsetIDUnique()) 
	    throw cms::Exception("MismatchedInput")
	      << "File " << filename
	      << "\nhas  " << i->second.psetIDs().size()
	      << "ParameterSetIDs"
	      << "\nfor branch " << i->first
	      << "\nand only one is allowed for strict merge\n";
      }
    }

    template <class T>
    bool
    readFromBranch_aux(TTree* tree,
		       std::string const& branchname,
		       Long64_t index,
		       T& thing) {
      assert(tree);
      TBranch* b = tree->GetBranch(branchname.c_str());
      if (!b) return false;
      T* thing_address = &thing;
      b->SetAddress(&thing_address);
      Int_t bytes_read = b->GetEntry(index);
      return (bytes_read > 0);
    }

    template <class T>
    void
    readFromBranch(TTree* tree,
		   std::string const& branchname,
		   Long64_t index,
		   char const* thingname,
		   std::string const& filename,
		   T& thing) {
      if (! readFromBranch_aux(tree, branchname, index, thing))
	throw cms::Exception("BadInputFile")
	  << "Failed to read "
	  << thingname
	  << " file: "
	  << filename 
	  << '\n';
    }

std::string
baseName(const std::string& s)
{
    std::string::size_type idx = s.rfind("/");
    if(idx == std::string::npos) return s;
    return s.substr(idx+1,std::string::npos);
}

void
listFilesInChain(TChain* chain)
{
  TObjArray* fiList = chain->GetListOfFiles();
  int numEntries = fiList->GetEntries();
  if(numEntries == 0) {
    std::cout << "\nTChain " << chain->GetName() << "has no files" << std::endl;
  } else {
    std::cout << "\nNumber of files in TChain " << chain->GetName() << " is "
              << numEntries << std::endl;
    for(int i = 0; i<numEntries; ++i) {
      TChainElement* ce = (TChainElement*)fiList->At(i);
      std::cout << "File name is " << baseName(ce->GetTitle())
                << "\tChain name is " << ce->GetName() << std::endl;
    }
  }
}

void
listOpenFiles()
{
  TFile* f = 0;
  std::cout << "\nList all open files" << std::endl;
  TIter next(gROOT->GetListOfFiles());
  while ((f = (TFile*)next())) {
    if(f->IsOpen()) {
      std::string s = f->GetName();
      std::string::size_type idx = s.rfind("/");
      std::cout << "There is an open file named " << s.substr(idx+1,std::string::npos)
                << "\tOption string is " << f->GetOption() << std::endl;
    }
  }
}

  } // end of anonymous namespace


  // TODO: use the Strategy pattern here to deal with differences in
  // testing modes.
  class ProcessInputFile {
  public:
    ProcessInputFile(std::string const& catalogName,
		     BranchDescription::MatchMode matchMode,
		     bool skipMissing);
    ~ProcessInputFile();
    void operator()(std::string const& fname, std::string const& logicalFileName);
    void merge(std::string const& outfilename,
	       std::string const& logicalFileName,
	       std::string const& catalogName,
	       pool::FileCatalog::FileID const& fid);
    TTree* fileMetaDataTree() { return fileMetaData_; }

    std::vector<std::string> const& branchNames() const { 
      return branchNames_;
    }

  private:
    std::string catalogURL_;
    Service<JobReport>   report_;    
    std::vector<JobReport::Token> inTokens_;
    std::auto_ptr<TFile> firstFile_;

    TTree*  fileMetaData_;  // not owned
    std::auto_ptr<TChain> eventData_;
    std::auto_ptr<TChain> eventMetaData_;
    std::auto_ptr<TChain> lumiData_;
    std::auto_ptr<TChain> lumiMetaData_;
    std::auto_ptr<TChain> runData_;
    std::auto_ptr<TChain> runMetaData_;
    BranchDescription::MatchMode matchMode_;
    bool skipMissing_;

    ProductRegistry          firstPreg_;
    std::vector<std::string> branchNames_;

    FileFormatVersion                                fileFormatVersion_;
    std::map<ParameterSetID, ParameterSetBlob>       parameterSetBlobs_;
    std::map<ProcessHistoryID, ProcessHistory>       processHistories_;
    std::map<ModuleDescriptionID, ModuleDescription> moduleDescriptions_;

    
    // helpers
    void merge_chains(TFile& outfile);

    // not implemented
    ProcessInputFile(ProcessInputFile const&);
    ProcessInputFile& operator=(ProcessInputFile const&);
  };		 

  ProcessInputFile::ProcessInputFile(
	std::string const& catalogName,
	BranchDescription::MatchMode matchMode,
	bool skipMissing) :
    catalogURL_(catalogName),
    report_(),
    inTokens_(),
    firstFile_(),
    fileMetaData_(),
    eventData_(),
    eventMetaData_(),
    lumiData_(),
    lumiMetaData_(),
    runData_(),
    runMetaData_(),
    matchMode_(matchMode),
    skipMissing_(skipMissing),
    firstPreg_(),
    branchNames_(),
    fileFormatVersion_(),
    parameterSetBlobs_(),
    processHistories_(),
    moduleDescriptions_()
  {}

  ProcessInputFile::~ProcessInputFile() {

    // I think we need to 'shut down' each tree that the TFile
    // firstFile_ is controlling.
    if (fileMetaData_) fileMetaData_->SetBranchAddress(poolNames::productDescriptionBranchName().c_str(), 
				    0);
  }
  

  // This operator is called to process each file. The steps of
  // processing for all but the first file are:

  //   1. Check the new file for consistency with the original
  //      file.
  //
  //      a. The MetaData trees must be compatible:
  //         i.   the file format version must be equal.
  //         ii.  the ProductRegistries must be equal.
  //         iii. the ModuleDescriptionMaps must be equal.
  //         iv.  the ProcessHistoryMaps must be equal.
  //         v.   we ignore the ParameterSetMaps.
  // 
  //   2. If the files are compatible, we record information about the
  //      new file.
  //      a. add the file name to the TChain for each tree we accumulate:
  //         i.   Event data
  //         ii.  Event meta data
  //         iii. lumi data
  //         iv.  run data
  //      b. deal with the POOL trees (???)
  //      c. update the objects stored from the MetaData tree
  //         i.  add any new ParameterSetBlobs to the ParameterSetMap

  void
  ProcessInputFile::operator()(std::string const& fname, std::string const& logicalFileName) {
    std::auto_ptr<TFile> currentFile(openTFile(fname, logicalFileName, false, skipMissing_));
    if (currentFile.get() == 0) {
      report_->reportSkippedFile(fname, logicalFileName);
      return;
    }
    bool first = (firstFile_.get() == 0);
  
      // --------------------
      // Test MetaData trees
      // --------------------
    TTree* currentFileMetaData = 
        getTTreeOrThrow(*currentFile, poolNames::metaDataTreeName());

    ProductRegistry currentProductRegistryBuffer;
    ProductRegistry & currentProductRegistry = (first ? firstPreg_ : currentProductRegistryBuffer);
    
    readFromBranch(currentFileMetaData, 
  		   poolNames::productDescriptionBranchName(),
  		   0, "ProductRegistry", fname, currentProductRegistry);    

    std::vector<std::string> currentBranchNamesBuffer;
    std::vector<std::string> & currentBranchNames = (first ? branchNames_ : currentBranchNamesBuffer);
    getBranchNamesFromRegistry(currentProductRegistry, currentBranchNames);

    // We delay any testing of compatibility until after we have
    // reported opening the new file.
  
    // FIXME: This input file open/close should be managed by a sentry
    // object.
    JobReport::Token inToken =
        report_->inputFileOpened(
  	    fname,          // physical filename
            logicalFileName,// logical filename
            catalogURL_,    // catalog
  	    "FastMerge",    // source class name
  	    "EdmFastMerge", // module label
  	    currentBranchNames);
  
    inTokens_.push_back(inToken);

    // TODO: refactor each of the following "clauses" to its own
    // member function. The previous one *might* need to remain as it
    // is, because of the special need to get the BranchNames before
    // reporting the opening of the file, and reporting the opening of
    // the file before doing the compatibility test.
  
    FileFormatVersion currentFileFormatVersion;
    readFromBranch(currentFileMetaData,
  		   poolNames::fileFormatVersionBranchName(),
  		   0, "FileFormatVersion", fname, currentFileFormatVersion);


    if (first) {
      fileMetaData_ = currentFileMetaData;
      fileFormatVersion_ = currentFileFormatVersion;
      if (fileFormatVersion_.value_ < 1)
        throw cms::Exception("MismatchedInput")
    	  << "This version of FastMerge only supports file version 1 or greater\n";

      TFile & curfile = *currentFile;
      eventData_     = makeTChainOrThrow(BranchTypeToProductTreeName(InEvent), curfile);
      eventMetaData_ = makeTChainOrThrow(BranchTypeToMetaDataTreeName(InEvent), curfile);
      lumiData_      = makeTChainOrThrow(BranchTypeToProductTreeName(InLumi), curfile);
      lumiMetaData_  = makeTChainOrThrow(BranchTypeToMetaDataTreeName(InLumi), curfile);
      runData_       = makeTChainOrThrow(BranchTypeToProductTreeName(InRun), curfile);
      runMetaData_   = makeTChainOrThrow(BranchTypeToMetaDataTreeName(InRun), curfile);

      checkStrictMergeCriteria(currentProductRegistry, getFileFormatVersion(), fname, matchMode_);
    } else {

      std::string mergeInfo = firstPreg_.merge(currentProductRegistry, fname, matchMode_);
      if (!mergeInfo.empty()) {
        throw cms::Exception("MismatchedInput")
	<< mergeInfo;
      }

      if (currentFileFormatVersion != fileFormatVersion_)
        throw cms::Exception("MismatchedInput")
  	<< "File format mismatch:"
  	<< "\nfirst file is version: " << fileFormatVersion_
  	<< "\nfile " << fname << " is version: " 
  	<< currentFileFormatVersion
  	<< '\n';

    }

    std::map<ModuleDescriptionID, ModuleDescription> currentModuleDescriptionsBuffer;
    std::map<ModuleDescriptionID, ModuleDescription> & currentModuleDescriptions =
        (first ? moduleDescriptions_ : currentModuleDescriptionsBuffer);
    readFromBranch(currentFileMetaData, 
  		   poolNames::moduleDescriptionMapBranchName(),
  		   0, "ModuleDescriptionMap", fname, 
  		   currentModuleDescriptions);

    std::map<ProcessHistoryID, ProcessHistory> currentProcessHistoriesBuffer;
    std::map<ProcessHistoryID, ProcessHistory> & currentProcessHistories =
        (first ? processHistories_ : currentProcessHistoriesBuffer);
    readFromBranch(currentFileMetaData, 
  		   poolNames::processHistoryMapBranchName(),
  		   0, "ProcessHistoryMap", fname, 
  		   currentProcessHistories);

    // TODO: Refactor the merging of the ParameterSet map to its own
    // private member function.
    std::map<ParameterSetID, ParameterSetBlob> currentParameterSetBlobsBuffer;
    std::map<ParameterSetID, ParameterSetBlob> & currentParameterSetBlobs =
        (first ? parameterSetBlobs_ : currentParameterSetBlobsBuffer);
    readFromBranch(currentFileMetaData, 
  		   poolNames::parameterSetMapBranchName(),
  		   0, "ParameterSetMap", fname, 
  		   currentParameterSetBlobs);

    if (first) {
      firstFile_ = currentFile;
    } else {
      moduleDescriptions_.insert(currentModuleDescriptions.begin(), currentModuleDescriptions.end());
  
      processHistories_.insert(currentProcessHistories.begin(), currentProcessHistories.end());
  
      //-----
      // The new file is now known to be compatible with previously read
      // files. Now we record the information about the new file.
      //-----
  
      { // block to limit scope some names ...
        typedef std::map<ParameterSetID,ParameterSetBlob> map_t;
        typedef map_t::key_type key_type;
        typedef map_t::value_type value_type;
        typedef map_t::const_iterator iter;
        for (iter i=currentParameterSetBlobs.begin(),
  	     e=currentParameterSetBlobs.end();
  	   i!=e; 
  	   ++i) {
  	  key_type const& thiskey = i->first;
  	  if (parameterSetBlobs_.find(thiskey) == parameterSetBlobs_.end())
  	    parameterSetBlobs_.insert(value_type(thiskey, i->second));
  	}
      } // end of block
    }    
    int nEventsBefore = eventMetaData_->GetEntries();
    int nEvents = 0;

    if (runMetaData_.get() != 0)   addFilenameToTChain(*runMetaData_, fname);
    if (runData_.get() != 0)       addFilenameToTChain(*runData_, fname);
    if (lumiMetaData_.get() != 0)  addFilenameToTChain(*lumiMetaData_, fname);
    if (lumiData_.get() != 0)      addFilenameToTChain(*lumiData_, fname);
    if (eventMetaData_.get() != 0) addFilenameToTChain(*eventMetaData_, fname);
    if (eventData_.get() != 0)     addFilenameToTChain(*eventData_, fname);

    nEvents = eventMetaData_->GetEntries() - nEventsBefore;
#ifdef VERBOSE
    std::cout << "\nnEvents for file " << baseName(fname) << " in chain runMetaData_ " << nEvents << std::endl;
#endif	// End VERBOSE

    // FIXME: This can report closure of the file even when
    // closing fails.
    report_->overrideEventsRead(inToken, nEvents);
    report_->inputFileClosed(inToken);
  }

  void
  ProcessInputFile::merge(std::string const& outfilename,
	       std::string const& logicalFileName,
	       std::string const& catalogName,
	       pool::FileCatalog::FileID const& fid) {
    // We are careful to open the file just before calling
    // merge_chains, because we must not allow alteration of ROOT's
    // global 'most recent file opened' state between creation of the
    // output TFile and use of that TFile by the TChain::Merge calls
    // we make in merge_chains. Isn't it delicious?

    std::auto_ptr<TFile> outFile(openTFile(outfilename, logicalFileName, true));

    // FIXME: This output file open/close should be managed by a
    // sentry object.
    JobReport::Token outToken = 
      report_->outputFileOpened(
	outfilename,    // physical filename
	logicalFileName,// logical filename
	catalogName,    // catalog
	"FastMerge",    // source class name
	"EdmFastMerge", // module label
	fid,		// File ID (guid)
	branchNames_);


    //----------
    // Write out file-level metadata
    //----------
    TTree* newMeta   = fileMetaDataTree()->CloneTree(0);

    FileFormatVersion *ffvp = &fileFormatVersion_;
    newMeta->SetBranchAddress(poolNames::fileFormatVersionBranchName().c_str(), &ffvp);

    std::map<ProcessHistoryID, ProcessHistory> *phmp = &processHistories_;
    newMeta->SetBranchAddress(poolNames::processHistoryMapBranchName().c_str(), &phmp);

    std::map<ModuleDescriptionID, ModuleDescription> *mdmp = &moduleDescriptions_;
    newMeta->SetBranchAddress(poolNames::moduleDescriptionMapBranchName().c_str(), &mdmp);

    ProductRegistry *pregp = &firstPreg_;
    newMeta->SetBranchAddress(poolNames::productDescriptionBranchName().c_str(), &pregp);

    std::map<ParameterSetID, ParameterSetBlob> *psetp = &parameterSetBlobs_;
    newMeta->SetBranchAddress(poolNames::parameterSetMapBranchName().c_str(), &psetp);

    newMeta->Fill();
    newMeta->Write();

    //----------
    // Merge the chains.
    //----------
    merge_chains(*outFile);

    int nEvents = eventData_->GetEntries();

    TFile &f = *outFile;
    TTree *tEvent = dynamic_cast<TTree *>(f.Get(BranchTypeToProductTreeName(InEvent).c_str()));
    if (tEvent) {
      tEvent->BuildIndex("id_.run_", "id_.event_");
    }
    TTree *tLumi = dynamic_cast<TTree *>(f.Get(BranchTypeToProductTreeName(InLumi).c_str()));
    if (tLumi) {
      tLumi->BuildIndex("id_.run_", "id_.luminosityBlock_");
    }
    TTree *tRun = dynamic_cast<TTree *>(f.Get(BranchTypeToProductTreeName(InRun).c_str()));
    if (tRun) {
      tRun->BuildIndex("id_.run_");
    }
    f.Write();
    f.Purge();

    TTree *lumitree = getTTreeOrThrow(f, BranchTypeToProductTreeName(InLumi));
    TBranch* lumiAux = lumitree->GetBranch(BranchTypeToAuxiliaryBranchName(InLumi).c_str());
    if (!lumiAux) {
      throw cms::Exception("RootFailure")
        << "Unable to find the TBranch: "
        << BranchTypeToAuxiliaryBranchName(InLumi)
        << "\n in TTree: "
        << BranchTypeToProductTreeName(InLumi)
        << '\n';
    }
    LuminosityBlockAuxiliary lbAux;
    LuminosityBlockAuxiliary *lbAuxPtr = &lbAux;
    lumiAux->SetAddress(&lbAuxPtr);
    int nLumis = lumiAux->GetEntries();
    for (int i = 0; i != nLumis; ++i) {
      lumiAux->GetEntry(i);
      report_->reportLumiSection(lbAux.run(), lbAux.luminosityBlock());
    }
    report_->overrideContributingInputs(outToken, inTokens_);
    report_->overrideEventsWritten(outToken, nEvents);
    report_->outputFileClosed(outToken);
  }

  void
  ProcessInputFile::merge_chains(TFile& outfile) {

#ifdef VERBOSE
    if (runMetaData_.get() != 0) {
      std::cout << "\nMerging runMetaData chains" << std::endl;
      mergeTChain(*runMetaData_, outfile);
      listOpenFiles();
      delete runMetaData_.release();
    }
    if (runData_.get() != 0) {
      std::cout << "\nMerging runData chains" << std::endl;
      mergeTChain(*runData_, outfile);
      listOpenFiles();
      delete runData_.release();
    }
    if (lumiMetaData_.get() != 0) {
      std::cout << "\nMerging lumiMetaData chains" << std::endl;
      mergeTChain(*lumiMetaData_, outfile);
      listOpenFiles();
      delete lumiMetaData_.release();
    }
    if (lumiData_.get() != 0) {
      std::cout << "\nMerging lumiData chains" << std::endl;
      mergeTChain(*lumiData_, outfile);
      listOpenFiles();
      delete lumiData_.release();
    }
    if (eventMetaData_.get() != 0) {
      std::cout << "\nMerging eventMetaData chains" << std::endl;
      mergeTChain(*eventMetaData_, outfile);
      listOpenFiles();
      delete eventMetaData_.release();
    }
    if (eventData_.get() != 0) {
      std::cout << "\nMerging eventData chains" << std::endl;
      mergeTChain(*eventData_, outfile);
      listOpenFiles();
    }
#else
    if (runMetaData_.get() != 0) {
      mergeTChain(*runMetaData_, outfile);
      delete runMetaData_.release();
    }
    if (runData_.get() != 0) {
      mergeTChain(*runData_, outfile);
      delete runData_.release();
    }
    if (lumiMetaData_.get() != 0) {
      mergeTChain(*lumiMetaData_, outfile);
      delete lumiMetaData_.release();
    }
    if (lumiData_.get() != 0) {
      mergeTChain(*lumiData_, outfile);
      delete lumiData_.release();
    }
    if (eventMetaData_.get() != 0) {
      mergeTChain(*eventMetaData_, outfile);
      delete eventMetaData_.release();
    }
    if (eventData_.get() != 0) mergeTChain(*eventData_, outfile);
#endif	// End VERBOSE
  }

  void
  FastMerge(std::vector<std::string> const& filesIn, 
	    std::string const& fileOut,
	    std::string const& catalogIn,
	    std::string const& catalogOut,
	    std::string const& lfnOut,
	    bool beStrict,
	    bool skipMissing) {

    if (fileOut.empty()) 
      throw cms::Exception("BadArgument")
	<< "no output file specified\n";

    if (filesIn.empty())
      throw cms::Exception("BadArgument")
	<< "no input files specified\n";

    std::string const empty;
    std::string const module = "FastMerge";

    // We don't know if we really have to reset this global state so
    // often, but that's part of the problem with global state!
    gErrorIgnoreLevel = kError;

    std::vector<std::string> branchNames;

    BranchDescription::MatchMode matchMode = (beStrict ? BranchDescription::Strict : BranchDescription::Permissive);

    ParameterSet pset;
    pset.addUntrackedParameter<std::vector<std::string> >("fileNames", filesIn);
    pset.addUntrackedParameter<std::string>("catalog", catalogIn);
    InputFileCatalog catalog(pset, skipMissing);
    ParameterSet opset;

    pool::Guid guid;
    pool::Guid::create(guid);
    std::string fid = guid.toString();

    std::vector<FileCatalogItem> const& inputFiles = catalog.fileCatalogItems();

    typedef std::vector<FileCatalogItem>::const_iterator iter;
    ProcessInputFile proc(catalog.url(), matchMode, skipMissing);

    // We don't use for_each, because we don't want our functor to be
    // copied.
    for (iter i=inputFiles.begin(), e=inputFiles.end(); i != e; ++i) proc(i->fileName(), i->logicalFileName());
    proc.merge(fileOut, lfnOut, catalogOut, fid);

  }

}
