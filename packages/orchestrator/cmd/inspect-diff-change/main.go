package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/e2b-dev/infra/packages/shared/pkg/storage"
	"github.com/e2b-dev/infra/packages/shared/pkg/storage/header"
	"github.com/google/uuid"
)

type Layer struct {
	BuildID string

	MemfileHeader *header.Header
	Memfile       storage.StorageObjectProvider
}

func main() {
	layersBuildIDsRaw := flag.String("layers", "", "all build layers' ids")
	blockSize := flag.Int64("block-size", 2097152, "block size")
	start := flag.Int64("start", 0, "start block")
	end := flag.Int64("end", 0, "end block")

	flag.Parse()

	layersBuildIDs := strings.Split(*layersBuildIDsRaw, ",")

	ctx := context.Background()

	s, err := storage.GetTemplateStorageProvider(ctx, nil)
	if err != nil {
		log.Fatalf("failed to get storage provider: %s", err)
	}

	layers := make([]Layer, 0, len(layersBuildIDs))

	getLayer := func(buildId string) *Layer {
		template := storage.TemplateFiles{
			BuildID: buildId,
		}

		memfileHeaderData, err := s.OpenObject(ctx, template.StorageMemfileHeaderPath())
		if err != nil {
			log.Fatalf("failed to open object: %s", err)
		}

		memfileHeader, err := header.Deserialize(memfileHeaderData)
		if err != nil {
			log.Fatalf("failed to deserialize memfile header: %s", err)
		}

		memfile, err := s.OpenObject(ctx, template.StorageMemfilePath())
		if err != nil {
			log.Fatalf("failed to open object: %s", err)
		}

		return &Layer{
			BuildID:       buildId,
			MemfileHeader: memfileHeader,
			Memfile:       memfile,
		}
	}

	for _, buildId := range layersBuildIDs {
		layer := getLayer(buildId)

		if len(layers) == 0 {
			baseLayer := getLayer(layer.MemfileHeader.Metadata.BaseBuildId.String())

			endBlock := int64(baseLayer.MemfileHeader.Metadata.Size) / *blockSize

			if *end == 0 || *end > endBlock {
				*end = endBlock
			}

			layers = append(layers, *baseLayer)
		}

		layers = append(layers, *layer)
	}

	for _, layer := range layers {
		log.Printf("layer %s generation: %d ", layer.BuildID, layer.MemfileHeader.Metadata.Generation)
	}

	// TODO: For every block, find the earliest accurence of the block value in the layers.

	fmt.Printf("Analyzing unchanged blocks for %d to %d by %d bytes blocks\n", *start, *end, *blockSize)

	var lastBlock []byte
	var lastGeneration uint64

	for offset := *start * (*blockSize); offset < *end*(*blockSize); offset += *blockSize {
		lastBlock = nil
		lastGeneration = 0

		fmt.Printf("\n[%012d-%012d] ", offset, offset+*blockSize)
		for _, layer := range layers {
			mappedOffset, _, buildId, err := layer.MemfileHeader.GetShiftedMapping(offset)
			if err != nil {
				log.Fatalf("failed to get shifted mapping: %s", err)
			}

			if buildId.String() == uuid.Nil.String() {
				fmt.Printf("000-")
				continue
			}

			if buildId.String() != layer.BuildID {
				fmt.Printf("xxx-")

				continue
			}

			block := make([]byte, *blockSize)

			_, err = layer.Memfile.ReadAt(block, mappedOffset)
			if err != nil {
				log.Fatalf("failed to read block: %s", err)
			}

			if bytes.Equal(block, lastBlock) {
				fmt.Printf("%03d-", lastGeneration)
			} else {
				fmt.Printf("%03d-", layer.MemfileHeader.Metadata.Generation)

				lastBlock = block
				lastGeneration = layer.MemfileHeader.Metadata.Generation
			}
		}

		fmt.Printf("|\n")
	}
}
