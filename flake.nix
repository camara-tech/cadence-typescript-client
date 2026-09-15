{
  description = "Dev environment for cadence-typescript-client";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            nodejs_22
            go
            protobuf
            grpcurl
            docker-compose
            jq
          ];
          shellHook = ''
            export CADENCE_GRPC_ADDR=127.0.0.1:7833
            export CADENCE_TCHANNEL_ADDR=127.0.0.1:7933
          '';
        };
      });
}
