#ifndef Common_FastMerge_h
#define Common_FastMerge_h

#include <vector>
#include <string>

namespace edm {
//  class PoolCatalog;
 // void FastMerge(PoolCatalog & catalog, std::vector<std::string> const& filesIn, std::string const& fileOut);
  void FastMerge(std::vector<std::string> const& filesIn, std::string const& fileOut);
}

#endif

