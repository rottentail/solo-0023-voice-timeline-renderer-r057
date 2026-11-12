package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"voice-timeline-renderer/internal/domain"
)

// SaveSource installs a complete source only. The media and metadata are
// always assembled in a staging directory and published with a single rename,
// so a failed write can never leave a source that is visible but unusable.
func (repo *Repository) SaveSource(asset domain.SourceAsset, wav []byte) error {
	repo.mu.Lock()
	defer repo.mu.Unlock()

	final := repo.path("sources", asset.ID)
	if _, err := os.Stat(final); err == nil {
		complete, completeErr := repo.completeSource(final)
		if completeErr != nil {
			return completeErr
		}
		if complete {
			return domain.E(domain.KindConflict, "source_conflict", "source_id", "source identifier already exists")
		}
		// An earlier registration failed after leaving a partial directory
		// behind. Reclaim it before installing the complete source: a source
		// that never finished registering must be registrable again once the
		// underlying storage recovers.
		if err := os.RemoveAll(final); err != nil {
			return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot remove incomplete source", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot inspect source destination", err)
	}

	return repo.installSource(final, asset, wav)
}

func (repo *Repository) installSource(final string, asset domain.SourceAsset, wav []byte) error {
	temp, err := os.MkdirTemp(repo.path("tmp"), "source-*")
	if err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot create source staging directory", err)
	}
	defer func() {
		_ = os.RemoveAll(temp)
	}()
	if err := writeBytes(filepath.Join(temp, "source.wav"), wav, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write source media", err)
	}
	if err := writeJSONAtomic(filepath.Join(temp, "metadata.json"), asset, 0o640); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot write source metadata", err)
	}
	if err := os.Rename(temp, final); err != nil {
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot commit source", err)
	}
	if err := syncDirectory(repo.path("sources")); err != nil {
		// Do not leave a published source behind for a request that reports
		// failure; a retry must be able to register it cleanly.
		_ = os.RemoveAll(final)
		return domain.Wrap(domain.KindInternal, "storage_error", "", "cannot sync source index", err)
	}
	return nil
}

// completeSource reports whether the directory is an internally consistent
// source: decodable metadata plus media bytes matching its recorded digest.
// Anything less is an interrupted registration, not a usable source.
func (repo *Repository) completeSource(final string) (bool, error) {
	metadata, err := os.ReadFile(filepath.Join(final, "metadata.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read source metadata", err)
	}
	var stored domain.SourceAsset
	if err := json.Unmarshal(metadata, &stored); err != nil {
		return false, nil
	}
	if stored.Digest == "" {
		return false, nil
	}
	payload, err := os.ReadFile(filepath.Join(final, "source.wav"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read source media", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]) == stored.Digest, nil
}

func (repo *Repository) GetSourceMeta(id string) (domain.SourceAsset, error) {
	repo.mu.RLock()
	defer repo.mu.RUnlock()
	var asset domain.SourceAsset
	if err := readJSON(repo.path("sources", id, "metadata.json"), &asset); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return domain.SourceAsset{}, domain.E(domain.KindNotFound, "source_not_found", "source_id", "source does not exist")
		}
		return domain.SourceAsset{}, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read source metadata", err)
	}
	return asset, nil
}

func (repo *Repository) GetSource(id string) (domain.SourceAsset, []byte, error) {
	asset, err := repo.GetSourceMeta(id)
	if err != nil {
		return domain.SourceAsset{}, nil, err
	}
	repo.mu.RLock()
	payload, readErr := os.ReadFile(repo.path("sources", id, "source.wav"))
	repo.mu.RUnlock()
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return domain.SourceAsset{}, nil, domain.E(domain.KindNotFound, "source_not_found", "source_id", "source media does not exist")
		}
		return domain.SourceAsset{}, nil, domain.Wrap(domain.KindInternal, "storage_error", "", "cannot read source media", readErr)
	}
	digest := sha256.Sum256(payload)
	if hex.EncodeToString(digest[:]) != asset.Digest {
		return domain.SourceAsset{}, nil, domain.E(domain.KindInternal, "source_integrity_error", "source_id", "stored source digest does not match metadata")
	}
	return asset, payload, nil
}
