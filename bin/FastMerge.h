#ifndef Common_FastMerge_h
#define Common_FastMerge_h

#include <vector>
#include <string>

namespace edm 
{
  void FastMerge(std::vector<std::string> const& filesIn, 
		 std::string const& fileOut,
		 bool beStrict);
}

#endif

