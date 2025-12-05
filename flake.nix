{
  description = "Rust project with ezci";

  inputs = {
    ezci.url = "git+ssh://git@git.manomano.tech/pulse/ezci.git";
  };

  outputs =
    inputs@{ ezci, ... }:
    ezci.inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      imports = [
        ezci.flakeModules.default
      ];

      perSystem =
        { ... }:
        {
          ezci.gitlab.ci.enable = true;
          # ezci.pre-commit.enable = true;

          ezci.project = {
            ecosystems = {
              rust = {
                enable = true;
                name = "log-consolidator-checker-rs";
                src = ./.;
              };
            };
          };
        };
    };
}
