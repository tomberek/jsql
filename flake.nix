{
  inputs.nixpkgs.url = "github:NixOS/nixpkgs";
  inputs.cook.url = "github:tomberek/cook";

  outputs =
    inputs:
    inputs.cook inputs {
      recipes.packages.go = {};
      recipes.devShells.default = {mkShell, go}: mkShell {
        name = "go-dev-shell";
        packages = [ go ];
        CGO_ENABLED = "1";
      };
    };
}
