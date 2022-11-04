#pragma once

#if defined _WIN32 || defined __CYGWIN__
#ifdef __GNUC__
#define MCAP_EXPORT __attribute__((dllexport))
#define MCAP_IMPORT __attribute__((dllimport))
#else
#define MCAP_EXPORT __declspec(dllexport)
#define MCAP_IMPORT __declspec(dllimport)
#endif
#ifdef MCAP_BUILDING_DLL
#define MCAP_PUBLIC MCAP_EXPORT
#else
#define MCAP_PUBLIC MCAP_IMPORT
#endif
#define MCAP_PUBLIC_TYPE MCAP_PUBLIC
#define MCAP_LOCAL
#else
#define MCAP_EXPORT __attribute__((visibility("default")))
#define MCAP_IMPORT
#if __GNUC__ >= 4
#define MCAP_PUBLIC __attribute__((visibility("default")))
#define MCAP_LOCAL __attribute__((visibility("hidden")))
#else
#define MCAP_PUBLIC
#define MCAP_LOCAL
#endif
#define MCAP_PUBLIC_TYPE
#endif
