"""
deploy.py — Build and push the MLB HR Props dashboard to GitHub Pages.

Usage:
  python deploy.py              # run pipeline then deploy
  python deploy.py --skip-pipeline  # deploy existing data.json only
  python deploy.py --dry-run   # build only, no git push

Mirrors the NBA pipeline deploy.py structure.
Repo expected at C:\\mlb-props (Windows) or ~/mlb-props (Mac/Linux).
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

# ── Config — edit these ───────────────────────────────────────────────────────
GITHUB_REPO   = "https://github.com/dm-2026/mlb-props.git"   # your repo URL
DEPLOY_BRANCH = "gh-pages"

# Files and folders to include in the deploy
DEPLOY_INCLUDES = [
    "index.html",
    "data/data.json",
    "og-image.png",
]

BUILD_DIR = Path("build")


def run_cmd(cmd, check=True, cwd=None):
    print(f"  $ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=check, cwd=cwd, capture_output=False)
    return result


def build():
    """Copy deploy files into build/ directory."""
    print("\n[1/3] Building…")
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir()

    for item in DEPLOY_INCLUDES:
        src = Path(item)
        dst = BUILD_DIR / src
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.exists():
            shutil.copy2(src, dst)
            print(f"  Copied {src} → {dst}")
        else:
            print(f"  [WARNING] {src} not found — skipping")

    # Write a .nojekyll so GitHub Pages serves data.json correctly
    (BUILD_DIR / ".nojekyll").touch()
    print("  Created .nojekyll")


def deploy(dry_run=False):
    """Push build/ to gh-pages branch."""
    print("\n[2/3] Deploying to GitHub Pages…")

    if dry_run:
        print("  --dry-run: skipping git push")
        print(f"  Build ready in: {BUILD_DIR.resolve()}")
        return

    # Commit any pending changes on main first
    run_cmd(["git", "add", "-A"])
    try:
        run_cmd(["git", "commit", "-m", "chore: update MLB HR props data"])
    except subprocess.CalledProcessError:
        print("  Nothing to commit on main — continuing")

    # ── Fetch remote gh-pages so we're in sync with GitHub Actions pushes ──────
    # This is the key step that prevents the "rejected — fetch first" error.
    print(f"  Fetching remote {DEPLOY_BRANCH}…")
    subprocess.run(
        ["git", "fetch", "origin", f"{DEPLOY_BRANCH}:{DEPLOY_BRANCH}"],
        capture_output=False
    )

    # ── Deploy via worktree — clean, no subtree history issues ─────────────────
    worktree_path = Path("_gh_pages_tmp")
    try:
        # Remove any stale worktree from a previous failed run
        subprocess.run(["git", "worktree", "remove", "--force", str(worktree_path)],
                       capture_output=True)
        if worktree_path.exists():
            shutil.rmtree(worktree_path, ignore_errors=True)

        # Check out the gh-pages branch into a temporary worktree
        run_cmd(["git", "worktree", "add", str(worktree_path), DEPLOY_BRANCH])

        # Clear existing content (the .git file must stay — it's the worktree link)
        for item in worktree_path.iterdir():
            if item.name == ".git":
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()

        # Copy fresh build into the worktree
        for f in BUILD_DIR.rglob("*"):
            if f.is_file():
                rel = f.relative_to(BUILD_DIR)
                dst = worktree_path / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(f, dst)

        # Commit and push
        run_cmd(["git", "add", "-A"], cwd=worktree_path)
        try:
            run_cmd(["git", "commit", "-m", "deploy: MLB HR props update"], cwd=worktree_path)
        except subprocess.CalledProcessError:
            print("  Build unchanged — nothing new to push")
            return

        run_cmd(["git", "push", "origin", DEPLOY_BRANCH], cwd=worktree_path)
        print(f"\n  ✓ Deployed to {DEPLOY_BRANCH}")
        print(f"  Dashboard: https://dm-2026.github.io/mlb-props/")

    finally:
        run_cmd(["git", "worktree", "remove", "--force", str(worktree_path)], check=False)
        if worktree_path.exists():
            shutil.rmtree(worktree_path, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser(description="Deploy MLB HR Props to GitHub Pages")
    parser.add_argument("--skip-pipeline", action="store_true", help="Skip running pipeline.py")
    parser.add_argument("--dry-run", action="store_true", help="Build only, no git push")
    args = parser.parse_args()

    print("=" * 50)
    print("  MLB HR Props — Deploy")
    print("=" * 50)

    if not args.skip_pipeline:
        print("\n[0/3] Running pipeline…")
        result = subprocess.run([sys.executable, "pipeline.py"])
        if result.returncode != 0:
            print("  Pipeline failed — aborting deploy")
            sys.exit(1)

    build()
    deploy(dry_run=args.dry_run)

    print("\n  Done.")


if __name__ == "__main__":
    main()
