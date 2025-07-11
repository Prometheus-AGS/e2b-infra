package edgepassthrough

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/metadata"

	"github.com/e2b-dev/infra/packages/proxy/internal/edge/sandboxes"
	"github.com/e2b-dev/infra/packages/shared/pkg/consts"
)

type (
	CleanupFunc  func(streamErr error)
	EventHandler func(ctx context.Context, rawHeader string) (CleanupFunc, error)
)

func makeCreateSandboxInCatalogHandler(catalog sandboxes.SandboxesCatalog) EventHandler {
	return func(ctx context.Context, rawHeader string) (CleanupFunc, error) {
		eventDecoded, err := base64.StdEncoding.DecodeString(rawHeader)
		if err != nil {
			return nil, fmt.Errorf("error base64 decoding catalog event: %w", err)
		}

		var event consts.SandboxCatalogCreateEvent
		err = json.Unmarshal(eventDecoded, &event)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling catalog event: %w", err)
		}

		sbxMaxLifetime := time.Duration(event.SandboxMaxLengthInHours) * time.Hour
		sbxStartedAt, err := time.Parse(time.RFC3339, event.SandboxStartTime)
		if err != nil {
			return nil, fmt.Errorf("error parsing sandbox start time: %w", err)
		}

		sbxRequest := &sandboxes.SandboxInfo{
			OrchestratorID:          event.OrchestratorID,
			ExecutionID:             event.ExecutionID,
			SandboxMaxLengthInHours: event.SandboxMaxLengthInHours,
			SandboxStartedAt:        sbxStartedAt,
		}

		err = catalog.StoreSandbox(event.SandboxID, sbxRequest, sbxMaxLifetime)
		if err != nil {
			return nil, fmt.Errorf("error storing sandbox in catalog: %w", err)
		}

		// When error during stream handling, we want to delete the sandbox from the catalog
		return func(streamErr error) {
			if streamErr != nil {
				return
			}

			err := catalog.DeleteSandbox(event.SandboxID, event.ExecutionID)
			if err != nil {
				zap.L().Error("error deleting sandbox from catalog when cleanup function", zap.Error(err))
			}
		}, nil
	}
}

func makeDeleteSandboxFromCatalogHandler(catalog sandboxes.SandboxesCatalog) EventHandler {
	return func(ctx context.Context, rawHeader string) (CleanupFunc, error) {
		eventDecoded, err := base64.StdEncoding.DecodeString(rawHeader)
		if err != nil {
			return nil, fmt.Errorf("error base64 decoding catalog event: %w", err)
		}

		var event consts.SandboxCatalogDeleteEvent
		err = json.Unmarshal(eventDecoded, &event)
		if err != nil {
			return nil, fmt.Errorf("error unmarshalling catalog event: %w", err)
		}

		err = catalog.DeleteSandbox(event.SandboxID, event.ExecutionID)
		if err != nil {
			zap.L().Error("error deleting sandbox from catalog", zap.Error(err))
		}

		return func(streamErr error) {
			// we don't need to do anything here for error during stream handling
		}, nil
	}
}

func (s *NodePassThroughServer) processEvents(ctx context.Context, md metadata.MD) ([]CleanupFunc, error) {
	var cleanupFns []CleanupFunc

	for eventType, handler := range s.eventHandlers {
		items := md.Get(eventType)
		if len(items) == 0 {
			continue
		}

		for _, rawHeader := range items {
			cleanup, err := handler(ctx, rawHeader)
			if err != nil {
				return nil, err // error processing event
			}

			if cleanup != nil {
				cleanupFns = append(cleanupFns, cleanup)
			}
		}
	}

	return cleanupFns, nil
}
