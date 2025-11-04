# Claude Code Behavior & Rules

## Language Usage Policy
- **Internal thinking**: Use English
- **User communication**: Use Japanese for all interactions, questions, answers, and messages directed to the user
- **Git operations**: Use English for commit messages, branch names, PR titles/descriptions, and all Git-related text

## Git Workflow Guidelines

### Pre-operation Checks
Before performing any Git-related operations, always:
1. Check current status with `git status`
2. Review recent commit history with `git log`

### Branch Strategy
- Follow **GitHub Flow** branching strategy
- Work on feature branches and merge to main via pull requests

### Commit Message Format
Follow semantic commit conventions:

**Structure:**
- **Subject line**: Brief, clear English statement explaining **WHY** the change was made
- **Body**: Bulleted list of **WHAT** was changed

**Example:**
```
fix: resolve authentication timeout issue

- Updated token refresh logic in auth service
- Increased timeout threshold from 30s to 60s
- Added retry mechanism for failed requests
```

**Semantic commit types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code refactoring
- `test`: Test additions or modifications
- `chore`: Maintenance tasks
- `perf`: Performance improvements
