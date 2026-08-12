/** Defines an MCAP_PUBLIC visibility attribute macro, which is used on all public interfaces.
 *  This can be defined before including `mcap.hpp` to directly control symbol visibility.
 *  If not defined externally, this library attempts to export symbols from the translation unit
 *  where MCAP_IMPLEMENTATION is defined, and import them anywhere else.
 */
#ifndef MCAP_PUBLIC
#  if defined _WIN32 || defined __CYGWIN__
#    ifdef MCAP_IMPLEMENTATION
#      ifdef __GNUC__
#        define MCAP_PUBLIC __attribute__((dllexport))
#      else
#        define MCAP_PUBLIC __declspec(dllexport)
#      endif
#    else
#      ifdef __GNUC__
#        define MCAP_PUBLIC __attribute__((dllimport))
#      else
#        define MCAP_PUBLIC __declspec(dllimport)
#      endif
#    endif
#  else
#    if __GNUC__ >= 4
#      define MCAP_PUBLIC __attribute__((visibility("default")))
#    else
#      define MCAP_PUBLIC
#    endif
#  endif
#endif

/** Macros to locally suppress deprecation warnings. The library uses these around its own
 *  internal reads of deprecated members, which must keep working to honor callers that still
 *  set them; consumers of the deprecated members get the warnings as usual.
 */
#if defined(_MSC_VER)
#  define MCAP_DIAGNOSTIC_PUSH __pragma(warning(push))
#  define MCAP_DIAGNOSTIC_POP __pragma(warning(pop))
#  define MCAP_IGNORE_DEPRECATED __pragma(warning(disable : 4996))
#else
#  define MCAP_DIAGNOSTIC_PUSH _Pragma("GCC diagnostic push")
#  define MCAP_DIAGNOSTIC_POP _Pragma("GCC diagnostic pop")
#  define MCAP_IGNORE_DEPRECATED _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#endif
