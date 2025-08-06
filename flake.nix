{
  description = "Nix flake for the sipin-transformator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:

    flake-utils.lib.eachDefaultSystem (system:
    let
      pkgs = import nixpkgs { inherit system; };
    in
    {
      devShells.default = pkgs.mkShell {
        venvDir = ".venv";
        LD_LIBRARY_PATH = "${pkgs.stdenv.cc.cc.lib}/lib";
        packages = with pkgs; [ 
          pyright
          python312 
          ruff
          jdk
        ] ++ (with pkgs.python312Packages; [
          pip
          venvShellHook
        ]);
      };
    });
}
