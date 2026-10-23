# Upload beside the remote project's existing minecraft.nix and server-scheduler.
# Uses that Nix file's Java/runtime setup and newly uploaded Scheduler binary.
{ pkgs ? import <nixpkgs> { } }:
let
  minecraftSetup = import ./minecraft.nix { inherit pkgs; };
  restart = pkgs.writeShellApplication {
    name = "restart-server-scheduler";
    runtimeInputs = [ pkgs.coreutils minecraftSetup.scheduler ];
    text = ''
      project_root="$PWD"
      if [[ $# -gt 0 ]]; then
        project_root="$1"
        shift
      fi
      if [[ $# -ne 0 ]]; then
        printf 'Usage: restart-server-scheduler [project-directory]\n' >&2
        exit 2
      fi
      project_root="$(realpath "$project_root")"
      runtime_config="$project_root/database/runtime-config.json"
      if [[ ! -r "$runtime_config" ]]; then
        printf 'Missing runtime config: %s. Start the project with minecraft.nix first.\n' "$runtime_config" >&2
        exit 1
      fi
      printf 'Restarting Scheduler and managed servers using %s\n' "$runtime_config"
      exec "${minecraftSetup.scheduler}/bin/server-scheduler" --config "$runtime_config" restart
    '';
    meta.mainProgram = "restart-server-scheduler";
  };
in
{
  default = restart;
  inherit restart;
}
