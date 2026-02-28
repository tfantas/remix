import * as cp from 'node:child_process'

/**
 * Check if a git tag exists
 */
export function tagExists(tag: string): boolean {
  try {
    cp.execFileSync('git', ['rev-parse', '--verify', `refs/tags/${tag}`], { stdio: 'pipe' })
    return true
  } catch {
    // Ignore and fall through to remote check.
  }

  try {
    cp.execFileSync('git', ['ls-remote', '--exit-code', '--tags', 'origin', `refs/tags/${tag}`], {
      stdio: 'pipe',
    })
    return true
  } catch {
    return false
  }
}
