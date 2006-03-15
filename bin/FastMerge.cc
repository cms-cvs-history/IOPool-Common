#include "IOPool/Common/bin/FastMerge.h"
#include "IOPool/Common/interface/PoolNames.h"
#include "DataFormats/Common/interface/ProductRegistry.h"
#include "FWCore/Utilities/interface/Exception.h"
#include "POOLCore/Guid.h"
#include "uuid/uuid.h"

#include "TChain.h"
#include "TError.h"

#include <string>

namespace edm {
  namespace {
    template<typename T>
    void
    compare_(TTree *lh, TTree *rh, TBranch *pb1, TBranch *pb2, int nEntries, std::string const& fileName) {
      T pr1;
      T pr2;
      T *ppReg1 = &pr1;
      T *ppReg2 = &pr2;
      pb1->SetAddress(&ppReg1);
      pb2->SetAddress(&ppReg2);
      for (int j = 0; j < nEntries; ++j) {
	pb1->GetEntry(j);
	pb2->GetEntry(j);
        if (pr1 != pr2) {
	  throw cms::Exception("MismatchedInput","FastMerge::compare_()")
          << "File " << fileName << "\nhas different product registry than previous files\n";

	}
      }
    }

    template<>
    void
    compare_<char>(TTree *lh, TTree *rh, TBranch *pb1, TBranch *pb2, int nEntries, std::string const& fileName) {
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
	  throw cms::Exception("MismatchedInput","FastMerge::compare_()")
          << "File " << fileName << "\nhas different " << rh->GetName() << " tree than previous files\n";
	}
      }
    }

    template<typename T>
    void
    compare(TTree *lh, TTree *rh, std::string const& fileName) {
      int nEntries = lh->GetEntries();
      if (nEntries != rh->GetEntries()) {
	std::cout << lh->GetName() << " # of entries does not match" << std::endl;
	return;
      }
      int nBranches = lh->GetNbranches();
      if (nBranches != rh->GetNbranches()) {
	std::cout << lh->GetName() << " # of branches does not match" << std::endl;
	return;
      }
      TObjArray *ta1 = lh->GetListOfBranches();
      TObjArray *ta2 = rh->GetListOfBranches();
      for (int i = 0; i < nBranches; ++i) {
	TBranch *pb1 = static_cast<TBranch *>(ta1->At(i));
	TBranch *pb2 = static_cast<TBranch *>(ta2->At(i));
	if (*pb1->GetName() != *pb2->GetName()) {
	  std::cout << pb1->GetName() << " name of branch does not match" << std::endl;
	  return;
	}
	if (*pb1->GetTitle() != *pb2->GetTitle()) {
	  std::cout << pb1->GetTitle() << " title of branch does not match" << std::endl;
	  return;
	}
	compare_<T>(lh, rh, pb1, pb2, nEntries, fileName);
      }
    }
  }
    
  // ---------------------
  void FastMerge(std::vector<std::string> const& filesIn, std::string const& fileOut) {
    if (!fileOut.empty()) {
      typedef std::vector<std::string> vstring;
      typedef vstring::const_iterator const_iterator;

      TTree *meta = 0;
      TTree *psets = 0;
      TTree *params = 0;
      TTree *shapes = 0;
      TTree *links = 0;

      std::string fileName;

      TChain events(poolNames::eventTreeName().c_str());
      gErrorIgnoreLevel = kError;
      for (const_iterator iter = filesIn.begin(); iter != filesIn.end(); ++iter) {
        // std::string pfn;
        // catalog.findFile(pfn, *iter);
        TFile *file = TFile::Open(iter->c_str());
        gErrorIgnoreLevel = kInfo;
	std::string fileName_ = file->GetName();

        TTree *meta_ = static_cast<TTree *>(file->Get(poolNames::metaDataTreeName().c_str()));
        assert(meta_);
        TTree *psets_ = static_cast<TTree *>(file->Get(poolNames::parameterSetTreeName().c_str()));
        TTree *params_ = static_cast<TTree *>(file->Get("##Params"));
        assert(params_);
        TTree *shapes_ = static_cast<TTree *>(file->Get("##Shapes"));
        assert(shapes_);
        TTree *links_ = static_cast<TTree *>(file->Get("##Links"));
        assert(links_);
        
        if (iter == filesIn.begin()) {
          meta = meta_;
          psets = psets_;
          params = params_;
          shapes = shapes_;
          links = links_;
	  fileName = fileName_;
        } else {
          compare<ProductRegistry>(meta, meta_, fileName);
          // compare<char>(params, params_, fileName);
          compare<char>(shapes, shapes_, fileName);
          compare<char>(links, links_, fileName);
        }
        events.Add(iter->c_str());
      }

      TFile *out = TFile::Open(fileOut.c_str(), "recreate", fileOut.c_str());

      TTree *newMeta = meta->CloneTree(-1, "fast");
      TTree *newPsets = psets->CloneTree(-1, "fast");
      TTree *newShapes = shapes->CloneTree(-1, "fast");
      TTree *newLinks = links->CloneTree(-1, "fast");

      TTree *newParams = params->CloneTree(0);
      Int_t nentries = (Int_t)params->GetEntries();
      std::string const fid("[NAME=FID][VALUE=");
      std::string const pfn("[NAME=PFN][VALUE=");
      char pr1[1024];
      memset(pr1, sizeof(pr1), '\0');
      params->SetBranchAddress("db_string", pr1);
      for (Int_t i = 0; i < nentries; ++i) {
        params->GetEntry(i);
        std::string entry = pr1;
	std::string::size_type idxFID = entry.find(fid);
	if (idxFID != std::string::npos) {
	  pool::Guid guid;
	  pool::Guid::create(guid); 
	  entry = fid + guid.toString() + "]";
	  memset(pr1, sizeof(pr1), '\0');
	  strcpy(pr1, entry.c_str());
	}
	std::string::size_type idxPFN = entry.find(pfn);
	if (idxPFN != std::string::npos) {
	  idxPFN += pfn.size();
	  entry = pfn + fileOut + "]";
	  memset(pr1, sizeof(pr1), '\0');
	  strcpy(pr1, entry.c_str());
	}
	newParams->Fill();
        memset(pr1, sizeof(pr1), '\0');
      }

      newMeta->Write();
      newPsets->Write();
      newParams->AutoSave();
      newShapes->Write();
      newLinks->Write();
      events.Merge(out, 32000, "fast");
    }
  }
}
