#include "logger.h"

#ifdef RELEASE
LogLevel Logger::global_level = LogLevel::QUIET;
#else
LogLevel Logger::global_level = LogLevel::DEBUGGING;
#endif
