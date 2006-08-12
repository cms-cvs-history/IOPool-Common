#ifndef Common_FastMerge_h
#define Common_FastMerge_h

#include <vector>
#include <string>

namespace edm 
{
  void FastMerge(std::vector<std::string> const& filesIn, 
		 std::string const& fileOut,
		 std::string const& catalogIn,
		 std::string const& catalogOut,
		 std::string const& lfn,
		 bool beStrict);
}

#endif

