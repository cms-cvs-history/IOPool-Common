#include "IOPool/Common/bin/FastMerge.h"

#include <memory>
#include <string>

#include "TChain.h"
#include "TError.h"
#include "TFile.h"
#include "TTree.h"
#include "TObjArray.h"
#include "TBranch.h"

#include "DataFormats/Common/interface/ProductRegistry.h"
#include "DataFormats/Common/interface/ParameterSetBlob.h"
#include "DataFormats/Common/interface/ProcessHistory.h"
#include "DataFormats/Common/interface/FileFormatVersion.h"
#include "DataFormats/Common/interface/ModuleDescription.h"
#include "FWCore/ParameterSet/interface/ParameterSet.h"
#include "FWCore/Framework/interface/FileCatalog.h"
#include "FWCore/Utilities/interface/GetFileFormatVersion.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "FWCore/MessageLogger/interface/JobReport.h"
#include "FWCore/MessageLogger/interface/MessageLogger.h"
#include "FWCore/ServiceRegistry/interface/Service.h"
#include "FWCore/Utilities/interface/PersistentNames.h"

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

namespace edm 
{
  namespace 
  {
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
	bool const noThrow = false)
    {
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
    getTTreeOrThrow(TFile& file, char const* treename)
    {
      TTree* result = dynamic_cast<TTree*>(file.Get(treename));
      if (!result )
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
    getTTreeOrThrow(TFile& file, std::string const& treename)
    {
      return getTTreeOrThrow(file, treename.c_str());
    }


    // Get a TTree of the given name from the already-open TFile, or
    // return a null pointer. Make sure not to return a non-null
    // pointer to a 'zombie' TTree.
    TTree*
    getTTree(TFile& file, char const* treename)
    {
      TTree* result = dynamic_cast<TTree*>(file.Get(treename));
      return ( result && !result->IsZombie()  )
	? result
	: 0;
    }

    // Create a TChain with the given name, or throw a suitable
    // exception.
    std::auto_ptr<TChain>
    makeTChainOrThrow(std::string const& name)
    {
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
    addFilenameToTChain(TChain& chain, std::string const& filename)
    {
      if ( chain.AddFile(filename.c_str()) == 0 )
	throw cms::Exception("RootFailure")
	  << "TChain::AddFile failed to add the file: "
	  << filename
	  << "\nto the TChain for TTree: "
	  << chain.GetName()
	  << '\n';
    }

    // Helper functions for the comparison of files.
    void
    compare_char(TTree* lh, TTree* rh, 
		 TBranch* pb1, TBranch* pb2, 
		 int nEntries, std::string const& fileName) 
    {
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
	    std::string const& fileName)
    {
      int nEntries = lh->GetEntries();
      if (nEntries != rh->GetEntries())
	{
	  throw cms::Exception("MismatchedInput")
	    << "Number of entries in TTree: "
	    << lh->GetName()
	    << "\nfrom file: "
	    << fileName
	    << "\ndoes not match that from the original file\n";
	}
      int nBranches = lh->GetNbranches();
      if (nBranches != rh->GetNbranches()) 
	{
	  throw cms::Exception("MismatchedInput")
	    << "Number of branches in TTree: "
	    << lh->GetName()
	    << "\nfrom file: "
	    << fileName
	    << "\ndoes not match that from the original file\n";
	}
      TObjArray* ta1 = lh->GetListOfBranches();
      TObjArray* ta2 = rh->GetListOfBranches();
      for (int i = 0; i < nBranches; ++i) 
	{
	  TBranch* pb1 = static_cast<TBranch*>(ta1->At(i));
	  TBranch* pb2 = static_cast<TBranch*>(ta2->At(i));
	  if (*pb1->GetName() != *pb2->GetName()) 
	    {
	      throw cms::Exception("MismatchedInput")
		<< "Names of branches in TTree: "
		<< lh->GetName()
		<< "\nfrom file: "
		<< fileName
		<< "\ndoes not match that from the original file\n";
	    }
	  if (*pb1->GetTitle() != *pb2->GetTitle()) 
	    {
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
			       std::vector<std::string>& names)
    {
      typedef ProductRegistry::ProductList prodlist;
      typedef prodlist::const_iterator iter;

      prodlist const& prods = reg.productList();
      for (iter i=prods.begin(), e=prods.end(); i!=e; ++i)
	{
	  i->second.init();     // may not be needed
	  names.push_back(i->second.branchName());
	}
    }

    void
    checkStrictMergeCriteria(ProductRegistry& reg, 
			     int fileFormatVersion,
			     std::string const& filename,
			     BranchDescription::MatchMode matchMode)
    {
     

      // This is suitable only for file format version 1.
      if ( fileFormatVersion != 1 )
	throw cms::Exception("MismatchedInput")
	  << "This version of checkStrictMergeCriteria"
	  << " only supports file version 1\n";

      if (matchMode == BranchDescription::Permissive) return;

      // We require exactly one 'ProcessConfigurationID' and one
      // 'ParameterSetID' for each branch in the file.
      typedef ProductRegistry::ProductList::const_iterator iter;
      ProductRegistry::ProductList const& prods = reg.productList();
      for (iter i=prods.begin(),e=prods.end(); i!=e; ++i)
	{
	  if (i->second.processConfigurationIDs().size() != 1)
	    throw cms::Exception("MismatchedInput")
	      << "File " << filename
	      << "\nhas " << i->second.processConfigurationIDs().size()
	      << " ProcessConfigurations"
	      << "\nfor branch " << i->first
	      << "\nand only one is allowed for strict merge\n";

	  if (! i->second.isPsetIDUnique() ) 
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
		       T& thing)
    {
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
		   T& thing)
    {
      if (! readFromBranch_aux(tree, branchname, index, thing) )
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
  class ProcessInputFile
  {
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
    TTree* paramsTree() { return params_; }
    TTree* shapesTree() { return shapes_; }
    TTree* linksTree() { return links_; }
    TTree* fileMetaDataTree() { return fileMetaData_; }

    std::vector<std::string> const& branchNames() const { 
      return branchNames_; }

  private:
    std::string catalogURL_;
    Service<JobReport>   report_;    
    std::vector<JobReport::Token> inTokens_;
    std::auto_ptr<TFile> firstFile_;

    TTree* params_;     // not owned
    TTree* shapes_;     // not owned
    TTree* links_ ;     // not owned

    TTree*  fileMetaData_;  // not owned
    std::auto_ptr<TChain> eventData_;
    std::auto_ptr<TChain> eventMetaData_;
    // std::auto_ptr<TChain> lumiData_;
    // std::auto_ptr<TChain> runData_;
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
    params_(),
    shapes_(),
    links_(),
    fileMetaData_(),
    eventData_(),
    eventMetaData_(),
    // lumiData_(),
    // runData_(),
    matchMode_(matchMode),
    skipMissing_(skipMissing),
    firstPreg_(),
    branchNames_(),
    fileFormatVersion_(),
    parameterSetBlobs_(),
    processHistories_(),
    moduleDescriptions_()
  {}

  ProcessInputFile::~ProcessInputFile()
  {

    // I think we need to 'shut down' each tree that the TFile
    // firstFile_ is controlling.
    if (fileMetaData_) fileMetaData_->SetBranchAddress(poolNames::productDescriptionBranchName().c_str(), 
				    0);
    std::string const db_string = "db_string";
    if (links_) links_->SetBranchAddress(db_string.c_str(), 0);
    if (shapes_) shapes_->SetBranchAddress(db_string.c_str(), 0);
    if (params_) params_->SetBranchAddress(db_string.c_str(), 0);
  }
  

  // This operator is called to process each file. The steps of
  // processing for all but the first file are:

  //   1. Check the new file for consistency with the original
  //      file.
  //
  //      a. two of the POOL trees must match: ##Links and ##Shapes;
  //         the third POOL tree ##Params is not compared
  //      b. The MetaData trees must be compatible:
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
  ProcessInputFile::operator()(std::string const& fname, std::string const& logicalFileName)
  {
    std::auto_ptr<TFile> currentFile(openTFile(fname, logicalFileName, false, skipMissing_));
    if (currentFile.get() == 0) {
      report_->reportSkippedFile(fname, logicalFileName);
      return;
    }
    bool first = (firstFile_.get() == 0);
      // --------------------
      // Test Pool trees
      // --------------------
    TTree* currentParams = getTTreeOrThrow(*currentFile, "##Params");
    TTree* currentShapes = getTTreeOrThrow(*currentFile, "##Shapes");
    TTree* currentLinks  = getTTreeOrThrow(*currentFile, "##Links");
  
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
      params_ = currentParams; // We don't actually test for equality of this tree...
      shapes_ = currentShapes;
      links_ = currentLinks;
      fileMetaData_ = currentFileMetaData;
      fileFormatVersion_ = currentFileFormatVersion;
      if (fileFormatVersion_.value_ != 1)
        throw cms::Exception("MismatchedInput")
    	  << "This version of FastMerge only supports file version 1\n";

      eventData_ = (makeTChainOrThrow(poolNames::eventTreeName()));
      eventMetaData_ = (makeTChainOrThrow(poolNames::eventMetaDataTreeName()));
      // lumiData_ = (makeTChainOrThrow(poolNames::luminosityBlockTreeName()));
      // runData_ = (makeTChainOrThrow(poolNames::runTreeName()));

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
  	<< "\nfile " << fname << " is versin: " 
  	<< currentFileFormatVersion
  	<< '\n';

      // These comparison functions throw on failure; we are not
      // neglecting a return value from 'compare'.
      compare(shapes_, currentShapes, fname);
      compare(links_, currentLinks, fname);
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
  	   ++i)
  	{
  	  key_type const& thiskey = i->first;
  	  if (parameterSetBlobs_.find(thiskey) == parameterSetBlobs_.end())
  	    parameterSetBlobs_.insert(value_type(thiskey, i->second));
  	}
      } // end of block
    }    
    int nEventsBefore = eventMetaData_->GetEntries();
    // addFilenameToTChain(*runData_, fname);
    // addFilenameToTChain(*lumiData_, fname);
    addFilenameToTChain(*eventData_, fname);
    addFilenameToTChain(*eventMetaData_, fname);
    int nEvents = eventMetaData_->GetEntries() - nEventsBefore;

    // FIXME: This can report closure of the file even when
    // closing fails.
    report_->overrideEventsRead(inToken, nEvents);
    report_->inputFileClosed(inToken);
  }

  void
  ProcessInputFile::merge(std::string const& outfilename,
	       std::string const& logicalFileName,
	       std::string const& catalogName,
	       pool::FileCatalog::FileID const& fid)
  {
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
    // Handle the POOL trees
    //----------
    TTree* newShapes = shapesTree()->CloneTree(-1, "fast");
    newShapes->Write();

    // There are mysterious problems with cloning the entries of the ##Links tree.
    // So, we copy the entries by hand.
    TTree* newLinks  = linksTree()->CloneTree(0);
    Long64_t mentries = linksTree()->GetEntries();
    char pr0[1024];
    memset(pr0, sizeof(pr0), '\0');
    linksTree()->SetBranchAddress("db_string", pr0);
    for (Long64_t j = 0; j < mentries; ++j) {
	linksTree()->GetEntry(j);
	newLinks->Fill();
        memset(pr0, sizeof(pr0), '\0');
    }
    newLinks->AutoSave();

    TTree* newParams = paramsTree()->CloneTree(0);
    Long64_t nentries = paramsTree()->GetEntries();
    std::string const fidPrefix("[NAME=FID][VALUE=");
    std::string const pfnPrefix("[NAME=PFN][VALUE=");
    char pr1[1024];
    memset(pr1, sizeof(pr1), '\0');
    paramsTree()->SetBranchAddress("db_string", pr1);
    for (Long64_t i = 0; i < nentries; ++i) {
	paramsTree()->GetEntry(i);
	std::string entry = pr1;
	std::string::size_type idxFID = entry.find(fidPrefix);
	if (idxFID != std::string::npos) {
	  entry = fidPrefix + fid + "]";
	  memset(pr1, sizeof(pr1), '\0');
	  strcpy(pr1, entry.c_str());
        }
	std::string::size_type idxPFN = entry.find(pfnPrefix);
	if (idxPFN != std::string::npos) {
	    idxPFN += pfnPrefix.size();
	    entry = pfnPrefix + outfilename + "]";
	    memset(pr1, sizeof(pr1), '\0');
	    strcpy(pr1, entry.c_str());
	}
	newParams->Fill();
	memset(pr1, sizeof(pr1), '\0');
    }
    newParams->AutoSave();



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
    report_->overrideContributingInputs(outToken, inTokens_);
    report_->overrideEventsWritten(outToken, nEvents);
    report_->outputFileClosed(outToken);
  }

  void
  ProcessInputFile::merge_chains(TFile& outfile)
  {
    Int_t const     basketsize(32000);

    // We have to specify 'keep' to prevent ROOT from calling delete
    // on the TFile* we pass to TChain::Merge; we specify 'fast' to
    // get ROOT to transfer raw data, rather than unzipping and
    // re-creating objects.
    Option_t const* opts("fast,keep");

    // runData_->Merge(&outfile, basketsize, opts);
    // lumiData_->Merge(&outfile, basketsize, opts);
    eventMetaData_->Merge(&outfile, basketsize, opts);
    eventData_->Merge(&outfile, basketsize, opts);
  }

    
  void
  FastMerge(std::vector<std::string> const& filesIn, 
	    std::string const& fileOut,
	    std::string const& catalogIn,
	    std::string const& catalogOut,
	    std::string const& lfnOut,
	    bool beStrict,
	    bool skipMissing)  
  {

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
    opset.addUntrackedParameter<std::string>("fileName", fileOut);
    opset.addUntrackedParameter<std::string>("logicalFileName", lfnOut);
    opset.addUntrackedParameter<std::string>("catalog", catalogOut);
    OutputFileCatalog outputCatalog(opset);
    pool::FileCatalog::FileID fid = outputCatalog.registerFile(fileOut, lfnOut);

    std::vector<FileCatalogItem> const& inputFiles = catalog.fileCatalogItems();

    typedef std::vector<FileCatalogItem>::const_iterator iter;
    ProcessInputFile proc(catalog.url(), matchMode, skipMissing);

    // We don't use for_each, because we don't want our functor to be
    // copied.
    for (iter i=inputFiles.begin(), e=inputFiles.end(); i != e; ++i) proc(i->fileName(), i->logicalFileName());
    proc.merge(fileOut, lfnOut, outputCatalog.url(), fid);

    outputCatalog.commitCatalog();
  }

}
