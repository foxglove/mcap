/** Defines an MCAP_PUBLIC visibility attribute macro, which is used on all public interfaces.
 *  This can be defined before including `mcap.hpp` to directly control symbol visibility.
 *  If not defined externally, this library attempts to export symbols from the translation unit
 *  where MCAP_IMPLEMENTATION is defined, and import them anywhere else.
 */
#ifndef MCAP_PUBLIC
#  if defined _WIN32 || defined __CYGWIN__
#    ifdef __GNUC__
#      define MCAP_EXPORT __attribute__((dllexport))
#      define MCAP_IMPORT __attribute__((dllimport))
#    else
#      define MCAP_EXPORT __declspec(dllexport)
#      define MCAP_IMPORT __declspec(dllimport)
#    endif
#    ifdef MCAP_IMPLEMENTATION
#      define MCAP_PUBLIC MCAP_EXPORT
#    else
#      define MCAP_PUBLIC MCAP_IMPORT
#    endif
#  else
#    define MCAP_EXPORT __attribute__((visibility("default")))
#    define MCAP_IMPORT
#    if __GNUC__ >= 4
#      define MCAP_PUBLIC __attribute__((visibility("default")))
#    else
#      define MCAP_PUBLIC
#    endif
#  endif
#endif
