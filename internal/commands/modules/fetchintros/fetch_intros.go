package fetchintros

import (
	"fmt"
	"gamerpal/internal/commands/types"
	"gamerpal/internal/database"
	"log"
	"time"

	"github.com/bwmarrin/discordgo"
)

// Module implements the CommandModule interface
type Module struct {
	deps *types.Dependencies
}

// New creates a new fetch-intros module
func New(deps *types.Dependencies) *Module {
	return &Module{
		deps: deps,
	}
}

// Register adds the fetch-intros command
func (m *Module) Register(cmds map[string]*types.Command, deps *types.Dependencies) {
	cmds["fetch-intros"] = &types.Command{
		ApplicationCommand: &discordgo.ApplicationCommand{
			Name:        "fetch-intros",
			Description: "Fetch all introduction posts from the forum and store in database",
		},
		HandlerFunc: m.handleFetchIntros,
	}
}

// Service returns nil (no background service needed)
func (m *Module) Service() types.ModuleService {
	return nil
}

// handleFetchIntros handles the /fetch-intros command
func (m *Module) handleFetchIntros(s *discordgo.Session, i *discordgo.InteractionCreate) {
	// Defer response (operation will take time)
	err := s.InteractionRespond(i.Interaction, &discordgo.InteractionResponse{
		Type: discordgo.InteractionResponseDeferredChannelMessageWithSource,
		Data: &discordgo.InteractionResponseData{
			Content: "Fetching introduction posts... This may take a minute.",
		},
	})
	if err != nil {
		log.Printf("Error deferring response: %v", err)
		return
	}

	// Get forum channel ID from config
	forumChannelID := m.deps.Config.GetGamerPalsIntroductionsForumChannelID()
	if forumChannelID == "" {
		m.editResponse(s, i, "❌ Error: Forum channel ID not configured")
		return
	}

	// Get guild ID from interaction
	guildID := i.GuildID

	// Fetch and store threads
	summary, err := m.fetchAndStoreThreads(s, guildID, forumChannelID)
	if err != nil {
		m.editResponse(s, i, fmt.Sprintf("❌ Error fetching threads: %v", err))
		return
	}

	m.editResponse(s, i, summary)
}

// fetchAndStoreThreads fetches threads from forum and stores in database
func (m *Module) fetchAndStoreThreads(s *discordgo.Session, guildID, forumChannelID string) (string, error) {
	// Use ForumCache to get all threads
	threads, ok := m.deps.ForumCache.ListThreads(forumChannelID)
	if !ok || threads == nil {
		// Cache miss - refresh forum
		err := m.deps.ForumCache.RefreshForum(guildID, forumChannelID)
		if err != nil {
			return "", fmt.Errorf("failed to refresh forum cache: %w", err)
		}
		threads, ok = m.deps.ForumCache.ListThreads(forumChannelID)
		if !ok {
			return "", fmt.Errorf("forum cache still empty after refresh")
		}
	}

	if len(threads) == 0 {
		return "⚠️ No threads found in forum", nil
	}

	// Fetch full content for each thread
	successCount := 0
	errorCount := 0
	skippedCount := 0

	for _, meta := range threads {
		// Skip archived threads older than 6 months (optional filter)
		if meta.Archived && time.Since(meta.CreatedAt) > 180*24*time.Hour {
			skippedCount++
			continue
		}

		// Fetch first message from thread (this is the introduction post)
		messages, err := s.ChannelMessages(meta.ID, 1, "", "", "")
		if err != nil {
			log.Printf("Failed to fetch messages for thread %s: %v", meta.ID, err)
			errorCount++
			continue
		}

		if len(messages) == 0 {
			log.Printf("No messages found in thread %s", meta.ID)
			errorCount++
			continue
		}

		firstMessage := messages[0]

		// Get username
		username := "Unknown"
		if firstMessage.Author != nil {
			username = firstMessage.Author.Username
		}

		// Create thread record
		thread := &database.IntroductionThread{
			ThreadID:            meta.ID,
			UserID:              meta.OwnerID,
			Username:            username,
			ThreadTitle:         meta.Name,
			FirstMessageContent: firstMessage.Content,
			CreatedAt:           meta.CreatedAt,
		}

		// Save to database
		err = m.deps.DB.SaveIntroductionThread(thread)
		if err != nil {
			log.Printf("Failed to save thread %s: %v", meta.ID, err)
			errorCount++
			continue
		}

		successCount++

		// Rate limit: Sleep 100ms between requests to avoid Discord API limits
		time.Sleep(100 * time.Millisecond)
	}

	// Build summary message
	summary := fmt.Sprintf(
		"✅ **Fetch Complete**\n\n"+
			"📊 **Summary:**\n"+
			"- Total threads found: **%d**\n"+
			"- Successfully stored: **%d**\n"+
			"- Errors: **%d**\n"+
			"- Skipped (old/archived): **%d**",
		len(threads), successCount, errorCount, skippedCount,
	)

	return summary, nil
}

// editResponse helper to update deferred response
func (m *Module) editResponse(s *discordgo.Session, i *discordgo.InteractionCreate, content string) {
	_, err := s.InteractionResponseEdit(i.Interaction, &discordgo.WebhookEdit{
		Content: &content,
	})
	if err != nil {
		log.Printf("Error editing response: %v", err)
	}
}
