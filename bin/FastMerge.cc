#include "IOPool/Common/bin/FastMerge.h"

#include <memory>

#include "Rtypes.h"
#include "TError.h"
#include "TFile.h"
#include "TObjArray.h"
#include "TBranch.h"
#include "TTree.h"
#include "TTreeCloner.h"

//#define VERBOSE

#include "FWCore/Catalog/interface/FileIdentifier.h"
#include "DataFormats/Provenance/interface/ProductRegistry.h"
#include "DataFormats/Provenance/interface/ParameterSetBlob.h"
#include "DataFormats/Provenance/interface/ProcessHistory.h"
#include "DataFormats/Provenance/interface/FileFormatVersion.h"
#include "DataFormats/Provenance/interface/FileID.h"
#include "DataFormats/Provenance/interface/BranchType.h"
#include "DataFormats/Provenance/interface/LuminosityBlockAuxiliary.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Catalog/interface/FileCatalog.h"
#include "FWCore/Catalog/interface/InputFileCatalog.h"
#include "FWCore/Utilities/interface/GetFileFormatVersion.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/MessageLogger/interface/JobReport.h"
#include "FWCore/ServiceRegistry/interface/Service.h"

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
      std::auto_ptr<TFile> result;
      if (noThrow) gErrorIgnoreLevel = kBreak;
      try {
        result = std::auto_ptr<TFile>(TFile::Open(filename.c_str(),option));
      } catch(cms::Exception e) {
	// Will rethrow below if noThrow is false.
      }
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
	  << "Unable to find file or unable to open file: " 
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
  } // end of anonymous namespace


  // TODO: use the Strategy pattern here to deal with differences in
  // testing modes.
  class ProcessInputFile {
  public:
    ProcessInputFile(std::string const& catalogName,
		     BranchDescription::MatchMode matchMode,
		     bool skipMissing,
		     std::string const& outName,
		     std::string const& logicalOutName,
		     std::string const& outCatalogName);
    ~ProcessInputFile();
    void operator()(FileCatalogItem const& item);
    void finalize();

  private:
    enum Trees {
	EVENT = 0,
	EVENTMETA = 1,
	LUMI = 2,
	LUMIMETA = 3,
	RUN = 4,
	RUNMETA = 5,
	END = 6
    };
    std::string catalogURL_;
    Service<JobReport>   report_;    
    std::vector<JobReport::Token> inTokens_;
    JobReport::Token outToken_;
    bool first_;

    TTree* fileMetaData_;  // Trees not owned
    BranchDescription::MatchMode matchMode_;
    bool skipMissing_;

    ProductRegistry          firstPreg_;
    std::vector<std::string> branchNames_;

    FileFormatVersion                                fileFormatVersion_;
    std::map<ParameterSetID, ParameterSetBlob>       parameterSetBlobs_;
    std::map<ProcessHistoryID, ProcessHistory>       processHistories_;
    std::map<ModuleDescriptionID, ModuleDescription> moduleDescriptions_;
    boost::shared_ptr<TFile> outFile_;
    std::string outFileName_;
    FileID fid_;
    std::string logicalOutFileName_;
    std::string outCatalogName_;
    std::vector<std::string> treeNames_;
    std::vector<TTree *> trees_;

    // not implemented
    ProcessInputFile(ProcessInputFile const&);
    ProcessInputFile& operator=(ProcessInputFile const&);
  };		 

  ProcessInputFile::ProcessInputFile(
	std::string const& catalogName,
	BranchDescription::MatchMode matchMode,
	bool skipMissing,
	std::string const& outFileName,
	std::string const& logicalOutFileName,
	std::string const& outCatalogName) :
    catalogURL_(catalogName),
    report_(),
    inTokens_(),
    outToken_(),
    first_(true),
    fileMetaData_(),
    matchMode_(matchMode),
    skipMissing_(skipMissing),
    firstPreg_(),
    branchNames_(),
    fileFormatVersion_(),
    parameterSetBlobs_(),
    processHistories_(),
    moduleDescriptions_(),
    outFile_(),
    outFileName_(outFileName),
    fid_(createFileIdentifier()),
    logicalOutFileName_(logicalOutFileName),
    outCatalogName_(outCatalogName),
    treeNames_(),
    trees_(END, 0)
  {
    treeNames_.reserve(END);
    treeNames_.push_back(BranchTypeToProductTreeName(InEvent));
    treeNames_.push_back(BranchTypeToMetaDataTreeName(InEvent));
    treeNames_.push_back(BranchTypeToProductTreeName(InLumi));
    treeNames_.push_back(BranchTypeToMetaDataTreeName(InLumi));
    treeNames_.push_back(BranchTypeToProductTreeName(InRun));
    treeNames_.push_back(BranchTypeToMetaDataTreeName(InRun));
  }

  ProcessInputFile::~ProcessInputFile() {}
  

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
  //   2. If the files are compatible, we:
  //      new file.
  //      a. use the tree cloner for each tree
  //         i.   Event data
  //         ii.  Event meta data
  //         iii. lumi data
  //         iv.  lumi meta data
  //         v.   run data
  //         vi.  run meta data
  //      c. update the objects stored from the MetaData tree
  //         i.  add any new ParameterSetBlobs to the ParameterSetMap

  void
  ProcessInputFile::operator()(FileCatalogItem const& item) {
    std::string const& fname = item.fileName();
    std::string const& logicalFileName = item.logicalFileName();
    std::auto_ptr<TFile> currentFile(openTFile(fname, logicalFileName, false, skipMissing_));
    if (currentFile.get() == 0) {
      report_->reportSkippedFile(fname, logicalFileName);
      return;
    }
      // --------------------
      // Test MetaData trees
      // --------------------
    TTree* currentFileMetaData = 
        getTTreeOrThrow(*currentFile, poolNames::metaDataTreeName());

    ProductRegistry currentProductRegistryBuffer;
    ProductRegistry & currentProductRegistry = (first_ ? firstPreg_ : currentProductRegistryBuffer);
    
    readFromBranch(currentFileMetaData, 
  		   poolNames::productDescriptionBranchName(),
  		   0, "ProductRegistry", fname, currentProductRegistry);    

    std::vector<std::string> currentBranchNamesBuffer;
    std::vector<std::string> & currentBranchNames = (first_ ? branchNames_ : currentBranchNamesBuffer);
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


    if (first_) {
      fileFormatVersion_ = currentFileFormatVersion;
      if (fileFormatVersion_.value_ < 1)
        throw cms::Exception("MismatchedInput")
    	  << "This version of FastMerge only supports file version 1 or greater\n";

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
        (first_ ? moduleDescriptions_ : currentModuleDescriptionsBuffer);
    readFromBranch(currentFileMetaData, 
  		   poolNames::moduleDescriptionMapBranchName(),
  		   0, "ModuleDescriptionMap", fname, 
  		   currentModuleDescriptions);

    std::map<ProcessHistoryID, ProcessHistory> currentProcessHistoriesBuffer;
    std::map<ProcessHistoryID, ProcessHistory> & currentProcessHistories =
        (first_ ? processHistories_ : currentProcessHistoriesBuffer);
    readFromBranch(currentFileMetaData, 
  		   poolNames::processHistoryMapBranchName(),
  		   0, "ProcessHistoryMap", fname, 
  		   currentProcessHistories);

    // TODO: Refactor the merging of the ParameterSet map to its own
    // private member function.
    std::map<ParameterSetID, ParameterSetBlob> currentParameterSetBlobsBuffer;
    std::map<ParameterSetID, ParameterSetBlob> & currentParameterSetBlobs =
        (first_ ? parameterSetBlobs_ : currentParameterSetBlobsBuffer);
    readFromBranch(currentFileMetaData, 
  		   poolNames::parameterSetMapBranchName(),
  		   0, "ParameterSetMap", fname, 
  		   currentParameterSetBlobs);

    if (!first_) {
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


    TFile & curfile = *currentFile;
    std::vector<TTree *> currentTrees;
    currentTrees.reserve(END);
    for (std::vector<std::string>::const_iterator it = treeNames_.begin(), itEnd = treeNames_.end();
	it != itEnd; ++it) {
      currentTrees.push_back(getTTree(curfile, it->c_str()));
    }

    if (first_) {
      outFile_ = boost::shared_ptr<TFile>(openTFile(outFileName_, logicalOutFileName_, true).release());
      // FIXME: This output file open/close should be managed by a sentry object.
      outToken_ = report_->outputFileOpened(
	  outFileName_,		// physical filename
	  logicalOutFileName_,	// logical filename
	  outCatalogName_,	// catalog
	  "FastMerge",		// source class name
	  "EdmFastMerge",	// module label
	  fid_.fid(),		// File ID (guid)
	  branchNames_);
    }

    outFile_->cd();
    for (int i = 0; i != END; ++i) {
      TTree *in = currentTrees[i];
      if (in != 0) {
	if(!trees_[i]) {
	  trees_[i] =  in->CloneTree(0); 
	}
	TTree *out = trees_[i];
	TTreeCloner cloner(in, out, "");
	if (!cloner.IsValid()) {
	  throw 0;
	}
	out->SetEntries(out->GetEntries() + in->GetEntries());
	cloner.Exec();
      }
    }
    if (fileMetaData_ == 0) {
	fileMetaData_ = currentFileMetaData->CloneTree(0);
    }

    // FIXME: This can report closure of the file even when closing fails.
    report_->overrideEventsRead(inToken, currentTrees[EVENT]->GetEntries());
    report_->inputFileClosed(inToken);
    curfile.Close();
    first_ = false;
  }

  void
  ProcessInputFile::finalize() {
     if (fileMetaData_ == 0) {
	throw cms::Exception("RootFailure")
	  << "Unable to find or open any input files.\n"; 
     }

    //----------
    // Write out file-level metadata
    //----------

    FileID *fidp = &fid_;
    if (fileMetaData_->GetBranch(poolNames::fileIdentifierBranchName().c_str())) {
      fileMetaData_->SetBranchAddress(poolNames::fileIdentifierBranchName().c_str(), &fidp);
    } else {
      fileMetaData_->Branch(poolNames::fileIdentifierBranchName().c_str(), &fidp);
    }

    FileFormatVersion *ffvp = &fileFormatVersion_;
    fileMetaData_->SetBranchAddress(poolNames::fileFormatVersionBranchName().c_str(), &ffvp);

    std::map<ProcessHistoryID, ProcessHistory> *phmp = &processHistories_;
    fileMetaData_->SetBranchAddress(poolNames::processHistoryMapBranchName().c_str(), &phmp);

    std::map<ModuleDescriptionID, ModuleDescription> *mdmp = &moduleDescriptions_;
    fileMetaData_->SetBranchAddress(poolNames::moduleDescriptionMapBranchName().c_str(), &mdmp);

    ProductRegistry *pregp = &firstPreg_;
    fileMetaData_->SetBranchAddress(poolNames::productDescriptionBranchName().c_str(), &pregp);

    std::map<ParameterSetID, ParameterSetBlob> *psetp = &parameterSetBlobs_;
    fileMetaData_->SetBranchAddress(poolNames::parameterSetMapBranchName().c_str(), &psetp);

    fileMetaData_->Fill();
    fileMetaData_->Write();


    int nEvents = trees_[EVENT]->GetEntries();

    TFile &f = *outFile_;
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
    report_->overrideContributingInputs(outToken_, inTokens_);
    report_->overrideEventsWritten(outToken_, nEvents);
    report_->outputFileClosed(outToken_);
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

    BranchDescription::MatchMode matchMode = (beStrict ? BranchDescription::Strict : BranchDescription::Permissive);

    ParameterSet pset;
    pset.addUntrackedParameter<std::vector<std::string> >("fileNames", filesIn);
    pset.addUntrackedParameter<std::string>("catalog", catalogIn);
    InputFileCatalog catalog(pset, skipMissing);

    std::vector<FileCatalogItem> const& inputFiles = catalog.fileCatalogItems();

    typedef std::vector<FileCatalogItem>::const_iterator iter;
    ProcessInputFile proc(catalog.url(), matchMode, skipMissing, fileOut, lfnOut, catalogOut);

    // We don't use for_each, because we don't want our functor to be copied.
    // for_all(inputFiles, proc).merge(fileOut, lfnOut, catalogOut);
    for (iter i=inputFiles.begin(), e=inputFiles.end(); i != e; ++i) proc(*i);
    proc.finalize();
  }

}
