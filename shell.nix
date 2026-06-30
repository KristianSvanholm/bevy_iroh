{ pkgs ? import <nixpkgs> { } }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    cacert
    rustc
    cargo
    rust-analyzer
    clang
  ];
}
