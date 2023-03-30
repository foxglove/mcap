{
  description = "MCAP nix flake";

  inputs = {
    nixpkgs.url      = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url  = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system}; in
      {
        packages = rec {
          mcap-cli = pkgs.buildGoModule rec {
            pname = "mcap-cli";
            version = "0.0.28";

            src = ./go/cli/mcap;

            vendorHash = "sha256-3TSwOgTIaBg5U8UdQ74LmsS8gS05Yw02JjfaIN9fKQQ=";

            # tests are currently failing because the test mcap files are in another directory in the repo
            doCheck = false;
          };
        };
      }
    );
}
