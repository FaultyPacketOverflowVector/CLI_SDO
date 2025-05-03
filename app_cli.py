import asyncio
import aiohttp
import aiofiles
import os
import vdf
import json
import zipfile
import argparse
import logging
from typing import List, Tuple, Dict, Optional

#################################################################
# Logging
#################################################################
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def linfo(message: str, *args) -> None:
    """Concise info logging"""
    logging.info(message, *args)

def lerror(message: str, *args) -> None:
    """Concise error logging"""
    logging.error(message, *args)

#################################################################
# Main Class
#################################################################
class DepotFileDownloader:
    def __init__(self) -> None:
        self._repos = self._load_repositories()
        
    def _load_repositories(self) -> Dict[str, str]:
        __fn_name__ = "_load_repositories"
        linfo("%s -> loading", __fn_name__)
        
        if os.path.exists("repositories.json"):
            with open("repositories.json", "r") as f:
                return json.load(f)
        
        return {}
    
    def _save_repositories(self) -> None:
        __fn_name__ = "_save_repositories"
        linfo("%s -> saving", __fn_name__)
        
        with open("repositories.json", "w") as f:
            json.dump(self._repos, f)
    
    async def _get(self, a_sha: str, a_path: str, a_repo: str) -> Optional[bytes]:
        __fn_name__ = "_get"
        linfo("%s -> %s", __fn_name__, a_path)
        
        v_url_list = [
            f"https://gcore.jsdelivr.net/gh/{a_repo}@{a_sha}/{a_path}",
            f"https://fastly.jsdelivr.net/gh/{a_repo}@{a_sha}/{a_path}",
            f"https://cdn.jsdelivr.net/gh/{a_repo}@{a_sha}/{a_path}",
            f"https://ghproxy.org/https://raw.githubusercontent.com/{a_repo}/{a_sha}/{a_path}",
            f"https://raw.dgithub.xyz/{a_repo}/{a_sha}/{a_path}",
        ]
        
        v_retry = 3
        async with aiohttp.ClientSession() as v_session:
            while v_retry > 0:
                for v_url in v_url_list:
                    try:
                        async with v_session.get(v_url, ssl=False) as v_r:
                            if v_r.status == 200:
                                return await v_r.read()
                            else:
                                lerror("%s -> status %d", __fn_name__, v_r.status)
                    except aiohttp.ClientError:
                        lerror("%s -> conn error", __fn_name__)
                    except KeyboardInterrupt:
                        lerror("%s -> interrupted", __fn_name__)
                        return None
                        
                v_retry -= 1
                linfo("%s -> retries %d", __fn_name__, v_retry)
                
        lerror("%s -> max retries", __fn_name__)
        return None
    
    #################################################################
    # Find and Download Files
    #################################################################    
    async def _get_manifest(self, a_sha: str, a_path: str, a_save_dir: str, a_repo: str) -> List[Tuple[str, str]]:
        __fn_name__ = "_get_manifest"
        linfo("%s -> %s", __fn_name__, a_path)
        
        v_collected_depots = []
        try:
            if a_path.endswith(".manifest"):
                v_save_path = os.path.join(a_save_dir, a_path)
                if os.path.exists(v_save_path):
                    linfo("%s -> exists", __fn_name__)
                    return v_collected_depots

                v_content = await self._get(a_sha, a_path, a_repo)
                if v_content is not None:
                    linfo("%s -> download success", __fn_name__)
                    async with aiofiles.open(v_save_path, "wb") as v_f:
                        await v_f.write(v_content)

            elif a_path in ["Key.vdf", "config.vdf"]:
                v_content = await self._get(a_sha, a_path, a_repo)
                if v_content is not None:
                    linfo("%s -> key success", __fn_name__)
                    v_depots_config = vdf.loads(v_content.decode(encoding="utf-8"))
                    for v_depot_id, v_depot_info in v_depots_config["depots"].items():
                        v_collected_depots.append((v_depot_id, v_depot_info["DecryptionKey"]))

        except KeyboardInterrupt:
            lerror("%s -> interrupted", __fn_name__)
            return []
        except Exception as v_e:
            lerror("%s -> %s", __fn_name__, type(v_e).__name__)
        
        return v_collected_depots
    
    async def _download_and_process(self, a_app_id: str, a_game_name: str, a_selected_repos: List[str]) -> Tuple[List[Tuple[str, str]], str]:
        __fn_name__ = "_download_and_process"
        linfo("%s -> %s", __fn_name__, a_app_id)
        
        try:
            v_app_id_list = list(filter(str.isdecimal, a_app_id.strip().split("-")))
            v_app_id = v_app_id_list[0]
            v_save_dir = f"./Games/{a_game_name} - {v_app_id}".replace(":", "").replace("|", "")
            os.makedirs(v_save_dir, exist_ok=True)

            for v_repo in a_selected_repos:
                linfo("%s -> repo %s", __fn_name__, v_repo)

                v_url = f"https://api.github.com/repos/{v_repo}/branches/{v_app_id}"
                async with aiohttp.ClientSession() as v_session:
                    try:
                        async with v_session.get(v_url, ssl=False) as v_r:
                            if v_r.status != 200:
                                lerror("%s -> repo inaccess", __fn_name__)
                                continue
                            
                            v_r_json = await v_r.json()
                            if "commit" in v_r_json:
                                v_sha = v_r_json["commit"]["sha"]
                                v_tree_url = v_r_json["commit"]["commit"]["tree"]["url"]
                                v_date = v_r_json["commit"]["commit"]["author"]["date"]
                                
                                async with v_session.get(v_tree_url, ssl=False) as v_r2:
                                    v_r2_json = await v_r2.json()
                                    if "tree" in v_r2_json:
                                        v_collected_depots = []

                                        v_vdf_paths = ["Key.vdf", "key.vdf", "config.vdf"]
                                        for v_vdf_path in v_vdf_paths:
                                            v_vdf_result = await self._get_manifest(v_sha, v_vdf_path, v_save_dir, v_repo)
                                            if v_vdf_result:
                                                v_collected_depots.extend(v_vdf_result)
                                                break

                                        for v_item in v_r2_json["tree"]:
                                            if v_item["path"].endswith(".manifest"):
                                                v_result = await self._get_manifest(v_sha, v_item["path"], v_save_dir, v_repo)
                                                if v_result:
                                                    v_collected_depots.extend(v_result)

                                        if v_collected_depots:
                                            linfo("%s -> updated %s", __fn_name__, v_date)
                                            linfo("%s -> storage success", __fn_name__)
                                            return v_collected_depots, v_save_dir
                    except aiohttp.ClientError as v_e:
                        lerror("%s -> %s", __fn_name__, type(v_e).__name__)
                    except KeyboardInterrupt:
                        lerror("%s -> interrupted", __fn_name__)
                        return [], v_save_dir

                linfo("%s -> not found", __fn_name__)

            lerror("%s -> all repos failed", __fn_name__)
            return [], v_save_dir
        except KeyboardInterrupt:
            lerror("%s -> interrupted", __fn_name__)
            return [], ""
    
    #################################################################
    # Output Dumped Files
    #################################################################
    def _parse_vdf_to_lua(self, a_depot_info: List[Tuple[str, str]], a_appid: str, a_save_dir: str) -> str:
        __fn_name__ = "_parse_vdf_to_lua"
        linfo("%s -> parsing", __fn_name__)
        
        v_lua_lines = []
        v_lua_lines.append(f"addappid({a_appid})")

        for v_depot_id, v_decryption_key in a_depot_info:
            v_lua_lines.append(f'addappid({v_depot_id},1,"{v_decryption_key}")')
            v_manifest_files = [
                f for f in os.listdir(a_save_dir)
                if f.startswith(v_depot_id + "_") and f.endswith(".manifest")
            ]
            for v_manifest_file in v_manifest_files:
                v_manifest_id = v_manifest_file[len(v_depot_id) + 1 : -len(".manifest")]
                v_lua_lines.append(f'setManifestid({v_depot_id},"{v_manifest_id}",0)')
        
        return "\n".join(v_lua_lines)
    
    def _zip_outcome(self, a_save_dir: str, a_selected_repos: List[str]) -> None:
        __fn_name__ = "_zip_outcome"
        linfo("%s -> zipping", __fn_name__)

        v_is_encrypted = any(self._repos[v_repo] == "Encrypted" for v_repo in a_selected_repos)

        v_save_dir = os.path.normpath(a_save_dir)

        v_zip_name = (
            os.path.basename(v_save_dir) + " - encrypted.zip"
            if v_is_encrypted
            else os.path.basename(v_save_dir) + ".zip"
        )
        v_zip_path = os.path.join(os.path.dirname(v_save_dir), v_zip_name)

        try:
            with zipfile.ZipFile(v_zip_path, "w", zipfile.ZIP_DEFLATED) as v_zipf:
                for v_root, v_dirs, v_files in os.walk(v_save_dir):
                    for v_file in v_files:
                        v_file_path = os.path.join(v_root, v_file)
                        v_arcname = os.path.relpath(v_file_path, start=v_save_dir)
                        v_zipf.write(v_file_path, v_arcname)

            for v_root, v_dirs, v_files in os.walk(v_save_dir, topdown=False):
                for v_file in v_files:
                    os.remove(os.path.join(v_root, v_file))
                for v_dir in v_dirs:
                    os.rmdir(os.path.join(v_root, v_dir))
            os.rmdir(v_save_dir)
            
            linfo("%s -> folder deleted", __fn_name__)
            linfo("%s -> zipped to %s", __fn_name__, v_zip_path)
            
        except FileNotFoundError:
            lerror("%s -> folder missing", __fn_name__)
        except OSError as v_e:
            lerror("%s -> %s", __fn_name__, type(v_e).__name__)
    
    async def _process_appid(self, a_appid: str, a_game_name: str) -> None:
        __fn_name__ = "_process_appid"
        linfo("%s -> %s", __fn_name__, a_appid)
        
        if not self._repos:
            lerror("%s -> no repos", __fn_name__)
            return

        v_selected_repo_list = list(self._repos.keys())
        
        linfo("%s -> download start", __fn_name__)
        linfo("%s -> using repos", __fn_name__)
        
        v_collected_depots, v_save_dir = await self._download_and_process(a_appid, a_game_name, v_selected_repo_list)
        
        if v_collected_depots:
            v_lua_script = self._parse_vdf_to_lua(v_collected_depots, a_appid, v_save_dir)
            v_lua_file_path = os.path.join(v_save_dir, f"{a_appid}.lua")
            try:
                async with aiofiles.open(v_lua_file_path, "w", encoding="utf-8") as v_lua_file:
                    await v_lua_file.write(v_lua_script)
                linfo("%s -> unlock gen success", __fn_name__)
            except Exception as v_e:
                lerror("%s -> write failed", __fn_name__)
            
            self._zip_outcome(v_save_dir, v_selected_repo_list)
            linfo("%s -> completed", __fn_name__)
        else:
            lerror("%s -> no depots", __fn_name__)

async def _main() -> None:
    __fn_name__ = "_main"
    linfo("%s -> starting", __fn_name__)
    
    v_parser = argparse.ArgumentParser(description='Steam Depot Downloader CLI')
    v_parser.add_argument('appid', help='Appid to download')
    v_parser.add_argument('--name', help='Game name (optional, defaults to appid)')
    v_args = v_parser.parse_args()
    
    v_game_name = v_args.name if v_args.name else f"Game {v_args.appid}"
    
    v_downloader = DepotFileDownloader()
    await v_downloader._process_appid(v_args.appid, v_game_name)

if __name__ == "__main__":
    asyncio.run(_main())
