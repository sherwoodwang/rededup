"""DuplicateMatch and related classes for comparing files and directories."""

from __future__ import annotations

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
    def from_dict(cls, data: dict[str, bool]) -> DuplicateMatchRule:
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
    """A duplicate found in the archive with metadata comparison results.

    Attributes:
        path: Path to the duplicate file or directory in the archive, relative to archive root.

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

        duplicated_size: Total size in bytes of files within this specific archive path
                        that have content-equivalent files in the analyzed path.
                        For files: the file size if content matches.
                        For directories: sum of all child file sizes that are content-equivalent.
                        NOTE: When a file in the analyzed path has multiple content-equivalent files in the archive,
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
                    For directories: True when duplicate contains all files from analyzed directory.
                    The duplicate may have additional files not in the analyzed directory.
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

        self.duplicated_items += match.duplicated_items
        self.duplicated_size += match.duplicated_size

    def aggregate_from_stat(self, analyzed_stat: os.stat_result, candidate_stat: os.stat_result) -> None:
        """Aggregate metadata matches by comparing two stat results.

        Args:
            analyzed_stat: stat result for the analyzed item
            candidate_stat: stat result for the candidate item
        """
        # Create a temporary DuplicateMatch with comparison results and aggregate from it
        mtime_match = analyzed_stat.st_mtime_ns == candidate_stat.st_mtime_ns
        atime_match = analyzed_stat.st_atime_ns == candidate_stat.st_atime_ns
        ctime_match = analyzed_stat.st_ctime_ns == candidate_stat.st_ctime_ns
        mode_match = analyzed_stat.st_mode == candidate_stat.st_mode
        owner_match = analyzed_stat.st_uid == candidate_stat.st_uid
        group_match = analyzed_stat.st_gid == candidate_stat.st_gid

        # Use a temporary match object to leverage aggregate_from_match logic
        temp_match = DuplicateMatch(
            Path('.'),  # Dummy path for aggregation purposes
            mtime_match=mtime_match,
            atime_match=atime_match,
            ctime_match=ctime_match,
            mode_match=mode_match,
            owner_match=owner_match,
            group_match=group_match,
            duplicated_size=0,
            duplicated_items=0
        )
        self.aggregate_from_match(temp_match)

    def create_duplicate_match(
            self,
            path: Path,
            *,
            non_identical: bool,
            non_superset: bool
    ) -> DuplicateMatch:
        """Create a DuplicateMatch using aggregated metadata and calculate is_identical.

        Args:
            path: Path to the duplicate (relative to archive root)
            non_identical: If True, forces is_identical to False (content/structure differs).
                          If False, calculates is_identical using the comparison rule.
            non_superset: If True, forces is_superset to False (not all items present).
                         If False, sets is_superset equal to is_identical.

        Returns:
            DuplicateMatch with aggregated metadata and calculated is_identical
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

        # Calculate is_identical: requires both structure match AND metadata match
        is_identical = (not non_identical) and metadata_matches

        # Calculate is_superset: requires all analyzed items present AND metadata match
        is_superset = (not non_superset) and metadata_matches

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
