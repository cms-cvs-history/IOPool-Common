#include "IOPool/Common/bin/FastMerge.h"
//#include "IOPool/Common/interface/PoolCatalog.h"
#include "IOPool/Common/interface/PoolNames.h"
//#include "DataFormats/Common/interface/ProductRegistry.h"

#include "TChain.h"

#include <iostream>
#include <string>


namespace edm {
   namespace {
      void compare(TTree *lh, TTree *rh) {
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
	}
      }
   }
  // ---------------------
//  void FastMerge(PoolCatalog & catalog, std::vector<std::string> const& filesIn, std::string const& fileOut) {
  void FastMerge(std::vector<std::string> const& filesIn, std::string const& fileOut) {
    if (!fileOut.empty()) {
      typedef std::vector<std::string> vstring;
      typedef vstring::const_iterator const_iterator;


      TTree *meta = 0;
      TTree *params = 0;
      TTree *shapes = 0;
      TTree *links = 0;


      TChain events(poolNames::eventTreeName().c_str());
      for (const_iterator iter = filesIn.begin(); iter != filesIn.end(); ++iter) {
        // std::string pfn;
        // catalog.findFile(pfn, *iter);
        TFile *file = TFile::Open(iter->c_str());

        TTree *meta_ = static_cast<TTree *>(file->Get(poolNames::metaDataTreeName().c_str()));
        assert(meta_);
        TTree *params_ = static_cast<TTree *>(file->Get("##Params"));
        assert(params_);
        TTree *shapes_ = static_cast<TTree *>(file->Get("##Shapes"));
        assert(shapes_);
        TTree *links_ = static_cast<TTree *>(file->Get("##Links"));
        assert(links_);
        
        if (iter == filesIn.begin()) {
          meta = meta_;
          params = params_;
          shapes = shapes_;
          links = links_;
        } else {
          //compare(meta, meta_);
          //compare(params, params_);
          //compare(shapes, shapes_);
          //compare(links, links_);
        }
        events.Add(iter->c_str());
      }

      TFile *out = TFile::Open(fileOut.c_str(), "create", fileOut.c_str());
      meta->Write();
      params->Write();
      shapes->Write();
      links->Write();

      events.Merge(out, 32000, "fast");
      // std::cout << "MERGED: " << i << std::endl;
    }
  }
}
