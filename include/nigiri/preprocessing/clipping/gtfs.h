#include "miniz.h"

#include "geo/polygon.h"

#include "nigiri/loader/dir.h"

namespace nigiri::preprocessing::clipping {

void clip_feed(loader::dir const&, geo::simple_polygon const&, mz_zip_archive&);

}
