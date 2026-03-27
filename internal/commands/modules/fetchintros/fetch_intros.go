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
	forumID := m.deps.Config.GetGamerPalsIntroductionsForumChannelID()
	if forumID == "" {
		m.editResponse(s, i, "❌ Error: Forum channel ID not configured")
		return
	}

	// Get guild ID from interaction
	guildID := i.GuildID

	// Fetch and store threads
	summary, err := m.fetchAndStoreThreads(s, guildID, forumID)
	if err != nil {
		m.editResponse(s, i, fmt.Sprintf("❌ Error fetching threads: %v", err))
		return
	}

	m.editResponse(s, i, summary)
}

// fetchAndStoreThreads fetches threads from forum and stores in database
func (m *Module) fetchAndStoreThreads(s *discordgo.Session, guildID, forumID string) (string, error) {
	// Use ForumCache to get all threads
	threads, ok := m.deps.ForumCache.ListThreads(forumID)
	if !ok || threads == nil {
		// Cache miss - refresh forum
		err := m.deps.ForumCache.RefreshForum(guildID, forumID)
		if err != nil {
			return "", fmt.Errorf("failed to refresh forum cache: %w", err)
		}
		threads, ok = m.deps.ForumCache.ListThreads(forumID)
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

	for _, meta := range threads {
		// Fetch the original message that created the thread
		// In Discord forum threads, the thread ID is the same as the first message ID
		firstMessage, err := s.ChannelMessage(meta.ID, meta.ID)
		if err != nil {
			log.Printf("Failed to fetch message for thread %s: %v", meta.ID, err)
			errorCount++
			continue
		}

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
			"- Errors: **%d**",
		len(threads), successCount, errorCount,
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
