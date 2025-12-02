"""DuplicateMatch and related classes for comparing files and directories."""

import os
from pathlib import Path
from typing import Union


class DuplicateMatchRule:
    """Defines which metadata properties are considered for exact identity matching.

    This rule determines which metadata fields must match for two files to be considered
    identical (beyond content equivalence). Different rules can be used for different
    analysis scenarios.

    Attributes:
        include_mtime: Whether modification time must match for identity
        include_atime: Whether access time must match for identity
        include_ctime: Whether change time must match for identity
        include_mode: Whether file permissions/mode must match for identity
        include_owner: Whether file owner (UID) must match for identity
        include_group: Whether file group (GID) must match for identity
    """

    def __init__(self, *, include_mtime: bool = True, include_atime: bool = False,
                 include_ctime: bool = False, include_mode: bool = True,
                 include_owner: bool = True, include_group: bool = True):
        self.include_mtime = include_mtime
        self.include_atime = include_atime
        self.include_ctime = include_ctime
        self.include_mode = include_mode
        self.include_owner = include_owner
        self.include_group = include_group

    def __eq__(self, other: object) -> bool:
        """Check if two rules are identical."""
        if not isinstance(other, DuplicateMatchRule):
            return False
        return (self.include_mtime == other.include_mtime and
                self.include_atime == other.include_atime and
                self.include_ctime == other.include_ctime and
                self.include_mode == other.include_mode and
                self.include_owner == other.include_owner and
                self.include_group == other.include_group)

    def __hash__(self) -> int:
        """Allow rule to be used as dict key."""
        return hash((self.include_mtime, self.include_atime, self.include_ctime,
                     self.include_mode, self.include_owner, self.include_group))

    def __repr__(self) -> str:
        """String representation for debugging."""
        included = []
        if self.include_mtime: included.append('mtime')
        if self.include_atime: included.append('atime')
        if self.include_ctime: included.append('ctime')
        if self.include_mode: included.append('mode')
        if self.include_owner: included.append('owner')
        if self.include_group: included.append('group')
        return f"DuplicateMatchRule({', '.join(included)})"

    def calculate_is_identical(self, *, mtime_match: bool, atime_match: bool,
                               ctime_match: bool, mode_match: bool,
                               owner_match: bool, group_match: bool) -> bool:
        """Calculate whether files are identical based on this rule.

        Args:
            mtime_match: Whether mtimes match
            atime_match: Whether atimes match
            ctime_match: Whether ctimes match
            mode_match: Whether modes match
            owner_match: Whether owners match
            group_match: Whether groups match

        Returns:
            True if all included metadata fields match
        """
        return ((not self.include_mtime or mtime_match) and
                (not self.include_atime or atime_match) and
                (not self.include_ctime or ctime_match) and
                (not self.include_mode or mode_match) and
                (not self.include_owner or owner_match) and
                (not self.include_group or group_match))

    def to_dict(self) -> dict[str, bool]:
        """Convert rule to dictionary for JSON serialization."""
        return {
            'include_mtime': self.include_mtime,
            'include_atime': self.include_atime,
            'include_ctime': self.include_ctime,
            'include_mode': self.include_mode,
            'include_owner': self.include_owner,
            'include_group': self.include_group
        }

    @classmethod
    def from_dict(cls, data: dict[str, bool]) -> "DuplicateMatchRule":
        """Load rule from dictionary."""
        return cls(
            include_mtime=data.get('include_mtime', True),
            include_atime=data.get('include_atime', False),
            include_ctime=data.get('include_ctime', False),
            include_mode=data.get('include_mode', True),
            include_owner=data.get('include_owner', True),
            include_group=data.get('include_group', True)
        )


class DuplicateMatch:
    """A duplicate found in the repository with metadata comparison results.

    Attributes:
        path: Path to the duplicate file or directory in the repository, relative to repository root.

        mtime_match: Whether modification times match exactly (nanosecond precision).
                     For files: compares the file's mtime directly.
                     For directories: True only if both the directory's mtime AND all child files'
                     mtimes match. False if ANY child file has mtime_match=False.

        atime_match: Whether access times match exactly (nanosecond precision).
                     For files: compares the file's atime directly.
                     For directories: True only if both the directory's atime AND all child files'
                     atimes match. False if ANY child file has atime_match=False.

        ctime_match: Whether change times (ctime) match exactly (nanosecond precision).
                     For files: compares the file's ctime directly.
                     For directories: True only if both the directory's ctime AND all child files'
                     ctimes match. False if ANY child file has ctime_match=False.

        mode_match: Whether file permissions/mode match exactly.
                    For files: compares the file's mode directly.
                    For directories: True only if both the directory's mode AND all child files'
                    modes match. False if ANY child file has mode_match=False.

        owner_match: Whether file owner (UID) matches exactly.
                     For files: compares the file's owner directly.
                     For directories: True only if both the directory's owner AND all child files'
                     owners match. False if ANY child file has owner_match=False.

        group_match: Whether file group (GID) matches exactly.
                     For files: compares the file's group directly.
                     For directories: True only if both the directory's group AND all child files'
                     groups match. False if ANY child file has group_match=False.

        duplicated_size: Total size in bytes of files within this specific repository path
                        that have content-equivalent files in the analyzed path.
                        For files: the file size if content matches.
                        For directories: sum of all child file sizes that are content-equivalent.
                        NOTE: When a file in the analyzed path has multiple content-equivalent files in the repository,
                        each DuplicateMatch counts that file's size independently.

        duplicated_items: Count of individual items that are duplicated.
                         - Regular files with matching content: count as 1
                         - Special files (symlinks, devices, pipes, sockets) with no difference: count as 1
                         - Directories themselves: count as 0 (only their contents are counted)
                         For directories: sum of duplicated_items from all matching children.

        is_identical: Whether duplicate_path is exactly identical to the analyzed item.
                     For files: True when all metadata matches (mtime_match AND atime_match AND ctime_match AND
                                mode_match AND owner_match AND group_match).
                     For directories: True when:
                       - All children match with identical metadata (is_identical=True for each)
                       - No extra files exist in either directory
                       - Directory-level metadata also matches

        is_superset: Whether duplicate_path contains all content from the analyzed item (possibly with extras).
                    For files: Always equals is_identical (files are atomic, no partial matches).
                    For directories: True when ALL of the following conditions are met:
                      - All analyzed files/directories are present in the duplicate (content-wise)
                      - All analyzed content has matching metadata (per the comparison rule)
                      - All child directories are also supersets of their analyzed counterparts
                      - The duplicate may contain additional files not in the analyzed directory
                    Note: This is strictly about content coverage with matching metadata, not just
                    structural presence. If any descendant file differs in content or metadata,
                    the entire ancestor chain will have is_superset=False.
    """

    def __init__(self, path: Path, *, mtime_match: bool,
                 atime_match: bool, ctime_match: bool, mode_match: bool,
                 owner_match: bool = False, group_match: bool = False,
                 duplicated_size: int = 0, duplicated_items: int = 0,
                 is_identical: bool = False, is_superset: bool = False,
                 rule: DuplicateMatchRule | None = None):
        self.path = path
        self.mtime_match = mtime_match
        self.atime_match = atime_match
        self.ctime_match = ctime_match
        self.mode_match = mode_match
        self.owner_match = owner_match
        self.group_match = group_match
        self.duplicated_size = duplicated_size
        self.duplicated_items = duplicated_items
        self.is_identical = is_identical
        self.is_superset = is_superset
        self.rule = rule


class MetadataMatchReducer:
    """Stateful reducer for computing metadata match results from stat comparisons.

    This class encapsulates the logic for reducing metadata comparison results
    from multiple stat comparisons (for child items, files, directories, or special files)
    down to aggregate match results. It also works for single stat comparisons.
    It starts with all metadata fields matching (True) and sets them to False if any
    comparison has a non-matching value.

    This implements an AND reduction pattern: all comparisons must have matching metadata
    for the final result to be considered matching.

    Attributes:
        mtime_match, atime_match, ctime_match, mode_match, owner_match, group_match:
            Metadata field match flags. Start as True and become False if any child has
            a non-matching value.

        duplicated_items: Count of items that matched (content-wise).
        duplicated_size: Total size of items that matched (content-wise).

        non_identical: True if any child has is_identical=False. This propagates up the
            directory tree to ensure parents are not marked identical when descendants differ.

        non_superset: True if any child has is_superset=False. This propagates up the
            directory tree to ensure parents are not marked as supersets when any descendant
            is not a superset (e.g., missing content or metadata mismatch).
    """

    def __init__(self, comparison_rule: DuplicateMatchRule) -> None:
        """Initialize reducer with all metadata matches set to True.

        Args:
            comparison_rule: Rule defining which metadata fields must match for identity
        """
        self.mtime_match: bool = True
        self.atime_match: bool = True
        self.ctime_match: bool = True
        self.mode_match: bool = True
        self.owner_match: bool = True
        self.group_match: bool = True
        self.duplicated_items: int = 0
        self.duplicated_size: int = 0
        self.non_identical: bool = False
        self.non_superset: bool = False
        self._comparison_rule: DuplicateMatchRule = comparison_rule

    def aggregate_from_match(self, match: Union[DuplicateMatch, None]) -> None:
        """Aggregate metadata matches from a DuplicateMatch.

        Args:
            match: DuplicateMatch to aggregate from
        """

        if match is None:
            return

        if not match.mtime_match:
            self.mtime_match = False
        if not match.atime_match:
            self.atime_match = False
        if not match.ctime_match:
            self.ctime_match = False
        if not match.mode_match:
            self.mode_match = False
        if not match.owner_match:
            self.owner_match = False
        if not match.group_match:
            self.group_match = False

        # Propagate non-identical and non-superset flags from children
        if not match.is_identical:
            self.non_identical = True
        if not match.is_superset:
            self.non_superset = True

        self.duplicated_items += match.duplicated_items
        self.duplicated_size += match.duplicated_size

    def aggregate_from_stat(self, analyzed_stat: os.stat_result, candidate_stat: os.stat_result) -> None:
        """Aggregate metadata matches by comparing two stat results.

        This method compares stat metadata fields and updates the reducer's match flags.
        It does NOT propagate is_identical/is_superset flags, as stat comparisons only
        provide metadata comparison results, not semantic identity/superset information.

        Args:
            analyzed_stat: stat result for the analyzed item
            candidate_stat: stat result for the candidate item
        """
        # Create a temporary DuplicateMatch with comparison results
        # IMPORTANT: Set is_identical=True and is_superset=True to prevent propagation
        # of non-identical/non-superset flags. We're only comparing metadata here.
        temp_match = DuplicateMatch(
            Path('.'),  # Dummy path for aggregation purposes
            mtime_match=analyzed_stat.st_mtime_ns == candidate_stat.st_mtime_ns,
            atime_match=analyzed_stat.st_atime_ns == candidate_stat.st_atime_ns,
            ctime_match=analyzed_stat.st_ctime_ns == candidate_stat.st_ctime_ns,
            mode_match=analyzed_stat.st_mode == candidate_stat.st_mode,
            owner_match=analyzed_stat.st_uid == candidate_stat.st_uid,
            group_match=analyzed_stat.st_gid == candidate_stat.st_gid,
            duplicated_size=0,
            duplicated_items=0,
            is_identical=True,  # Don't propagate non-identical flag
            is_superset=True    # Don't propagate non-superset flag
        )
        self.aggregate_from_match(temp_match)

    def create_duplicate_match(
            self,
            path: Path,
            *,
            non_identical: bool,
            non_superset: bool
    ) -> DuplicateMatch:
        """Create a DuplicateMatch using aggregated metadata and calculate identity/superset flags.

        Args:
            path: Path to the duplicate (relative to repository root)
            non_identical: If True, forces is_identical to False (structural mismatch at this level).
                          For directories, this is True when the set of immediate child names differs.
                          For files, this is typically False (files are compared by content).
            non_superset: If True, forces is_superset to False (not all analyzed items present at this level).
                         For directories, this is True when analyzed children are not a subset of
                         candidate children (i.e., some analyzed items are missing from the candidate).

        Returns:
            DuplicateMatch with aggregated metadata and calculated is_identical/is_superset flags.

            is_identical is True only when:
              - non_identical is False (structure matches at this level)
              - self.non_identical is False (no child has differences)
              - metadata_matches is True (all required metadata fields match)

            is_superset is True only when:
              - non_superset is False (all analyzed items present at this level)
              - self.non_superset is False (all children are supersets)
              - metadata_matches is True (all required metadata fields match)
        """
        # Calculate metadata match using comparison rule
        metadata_matches = self._comparison_rule.calculate_is_identical(
            mtime_match=self.mtime_match,
            atime_match=self.atime_match,
            ctime_match=self.ctime_match,
            mode_match=self.mode_match,
            owner_match=self.owner_match,
            group_match=self.group_match
        )

        # Calculate is_identical: requires structure match AND metadata match AND no non-identical children
        # All three conditions must be satisfied:
        # 1. non_identical parameter is False (structural match at this level)
        # 2. self.non_identical is False (no child has differences)
        # 3. metadata_matches is True (all required metadata fields match)
        is_identical = (not non_identical) and (not self.non_identical) and metadata_matches

        # Calculate is_superset: requires all analyzed items present AND metadata match AND children are supersets
        # All three conditions must be satisfied:
        # 1. non_superset parameter is False (all analyzed items present at this level)
        # 2. self.non_superset is False (all children are supersets)
        # 3. metadata_matches is True (all required metadata fields match)
        is_superset = (not non_superset) and (not self.non_superset) and metadata_matches

        return DuplicateMatch(
            path,
            mtime_match=self.mtime_match,
            atime_match=self.atime_match,
            ctime_match=self.ctime_match,
            mode_match=self.mode_match,
            owner_match=self.owner_match,
            group_match=self.group_match,
            duplicated_size=self.duplicated_size,
            duplicated_items=self.duplicated_items,
            is_identical=is_identical,
            is_superset=is_superset,
            rule=self._comparison_rule
        )
