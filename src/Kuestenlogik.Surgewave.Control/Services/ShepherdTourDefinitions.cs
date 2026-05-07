using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

public static class ShepherdTourDefinitions
{
    public static readonly ShepherdTourDefinition Welcome = new()
    {
        Id = "welcome",
        Title = "Welcome to Surgewave Control",
        Steps =
        [
            new()
            {
                Id = "welcome-intro",
                Title = "Welcome to Surgewave Control",
                Text = "Surgewave Control is your central management UI for Surgewave, a high-performance Kafka-compatible streaming platform. Let's take a quick tour!"
            },
            new()
            {
                Id = "welcome-dashboard",
                Title = "Dashboard Overview",
                Text = "The dashboard shows cluster health, throughput, broker count, topics, and consumer groups at a glance. Cards are drag-and-drop reorderable.",
                Element = "[data-tour-id='dashboard-summary']",
                Position = "bottom"
            },
            new()
            {
                Id = "welcome-sidebar",
                Title = "Navigation Sidebar",
                Text = "Use the sidebar to navigate between Topics, Consumer Groups, Connectors, Pipelines, Security, Monitoring, and more.",
                Element = ".mud-drawer",
                Position = "right"
            },
            new()
            {
                Id = "welcome-dark-mode",
                Title = "Dark Mode Toggle",
                Text = "Switch between dark and light mode with this button.",
                Element = "[data-tour-id='dark-mode-toggle']",
                Position = "bottom"
            },
            new()
            {
                Id = "welcome-tour-button",
                Title = "Tour Button",
                Text = "You can restart this tour or access page-specific tours anytime by clicking the tour button.",
                Element = "[data-tour-id='tour-button']",
                Position = "bottom"
            },
            new()
            {
                Id = "welcome-done",
                Title = "You're All Set!",
                Text = "Explore the sidebar to discover all features. Visit the Help Center for keyboard shortcuts, guides, and more. Happy streaming!"
            }
        ]
    };

    public static readonly ShepherdTourDefinition Topics = new()
    {
        Id = "topics",
        Title = "Topic Management",
        Steps =
        [
            new()
            {
                Id = "topics-intro",
                Title = "Topic Management",
                Text = "This page lets you browse, search, create, and delete topics in your Surgewave cluster."
            },
            new()
            {
                Id = "topics-search",
                Title = "Search Topics",
                Text = "Use the search box to quickly filter topics by name. The search is instant with debounce support.",
                Element = "[data-tour-id='topic-search']",
                Position = "bottom"
            },
            new()
            {
                Id = "topics-internal-toggle",
                Title = "Internal Topics Toggle",
                Text = "Toggle this switch to show or hide internal system topics (prefixed with __).",
                Element = "[data-tour-id='internal-topics-toggle']",
                Position = "bottom"
            },
            new()
            {
                Id = "topics-create",
                Title = "Create Topic",
                Text = "Click here to create a new topic. You can set the name, partition count, replication factor, and custom configurations.",
                Element = "[data-tour-id='create-topic-btn']",
                Position = "left"
            },
            new()
            {
                Id = "topics-details",
                Title = "Topic Details",
                Text = "Click on any topic name in the table to view its details, browse messages, produce test messages, and manage configurations."
            }
        ]
    };

    public static readonly ShepherdTourDefinition Consumers = new()
    {
        Id = "consumers",
        Title = "Consumer Monitoring",
        Steps =
        [
            new()
            {
                Id = "consumers-intro",
                Title = "Consumer Groups",
                Text = "This page shows all consumer groups in your cluster with their state, protocol type, and member count."
            },
            new()
            {
                Id = "consumers-search",
                Title = "Search Groups",
                Text = "Filter consumer groups by name using the search box.",
                Element = "[data-tour-id='consumer-search']",
                Position = "bottom"
            },
            new()
            {
                Id = "consumers-state-filter",
                Title = "State Filter",
                Text = "Filter groups by state: Stable, PreparingRebalance, CompletingRebalance, Empty, or Dead.",
                Element = "[data-tour-id='state-filter']",
                Position = "bottom"
            },
            new()
            {
                Id = "consumers-lag",
                Title = "Consumer Lag Dashboard",
                Text = "For detailed lag monitoring with time-series charts and offset reset, visit the Consumer Lag Dashboard from the sidebar."
            }
        ]
    };

    public static readonly ShepherdTourDefinition Connectors = new()
    {
        Id = "connectors",
        Title = "Kafka Connect",
        Steps =
        [
            new()
            {
                Id = "connectors-intro",
                Title = "Kafka Connect",
                Text = "Manage your Kafka Connect connectors here. Deploy source and sink connectors to integrate external systems."
            },
            new()
            {
                Id = "connectors-create",
                Title = "Create Connector",
                Text = "Click here to deploy a new connector. Configure it with a JSON configuration or select from available plugins.",
                Element = "[data-tour-id='create-connector-btn']",
                Position = "left"
            },
            new()
            {
                Id = "connectors-actions",
                Title = "Connector Actions",
                Text = "Each connector can be paused, resumed, restarted, or deleted. View its configuration with the settings icon."
            },
            new()
            {
                Id = "connectors-plugins",
                Title = "Available Plugins",
                Text = "Switch to the 'Available Plugins' tab to see installed connector plugins and deploy them with one click."
            }
        ]
    };

    public static readonly ShepherdTourDefinition Pipelines = new()
    {
        Id = "pipelines",
        Title = "Pipeline Editor",
        Steps =
        [
            new()
            {
                Id = "pipelines-intro",
                Title = "Pipeline Editor",
                Text = "The visual pipeline editor lets you build data processing pipelines by connecting source, transform, and sink nodes."
            },
            new()
            {
                Id = "pipelines-drag-drop",
                Title = "Drag & Drop Nodes",
                Text = "Drag nodes from the palette on the left onto the canvas. Connect them by dragging from output ports to input ports."
            },
            new()
            {
                Id = "pipelines-monitoring",
                Title = "Pipeline Monitoring",
                Text = "Once deployed, pipelines show real-time throughput, latency, and error metrics on each node."
            },
            new()
            {
                Id = "pipelines-lineage",
                Title = "Data Lineage",
                Text = "Visit the Pipeline Lineage page from the sidebar to visualize topic-to-topic data flow across all pipelines."
            }
        ]
    };

    public static readonly ShepherdTourDefinition Security = new()
    {
        Id = "security",
        Title = "Security & ACLs",
        Steps =
        [
            new()
            {
                Id = "security-intro",
                Title = "ACL Management",
                Text = "Access Control Lists (ACLs) let you control who can read, write, or manage specific resources in your cluster."
            },
            new()
            {
                Id = "security-add-acl",
                Title = "Add ACL",
                Text = "Click here to create a new ACL rule. Specify the principal, resource type, operation, and permission (Allow/Deny).",
                Element = "[data-tour-id='create-acl-btn']",
                Position = "left"
            },
            new()
            {
                Id = "security-filter",
                Title = "Filter & Search",
                Text = "Use the filters to narrow down ACLs by resource type, principal, or resource name. The table also supports full-text search."
            },
            new()
            {
                Id = "security-roles",
                Title = "Role Management",
                Text = "For role-based access control (RBAC), visit the Role Management page from the Security section in the sidebar."
            }
        ]
    };

    public static readonly ShepherdTourDefinition Monitoring = new()
    {
        Id = "monitoring",
        Title = "Monitoring & Alerts",
        Steps =
        [
            new()
            {
                Id = "monitoring-metrics",
                Title = "Metrics Dashboard",
                Text = "The metrics dashboard shows real-time cluster metrics including throughput, latency, disk usage, and more."
            },
            new()
            {
                Id = "monitoring-alerts",
                Title = "Alerting System",
                Text = "Set up alert rules for consumer lag, error rates, broker health, and other metrics. Configure notification channels like Slack, Email, or PagerDuty."
            },
            new()
            {
                Id = "monitoring-advisor",
                Title = "Performance Advisor",
                Text = "The Performance Advisor automatically analyzes your cluster and provides recommendations for hot partitions, consumer lag, and configuration tuning."
            },
            new()
            {
                Id = "monitoring-cluster",
                Title = "Cluster Overview",
                Text = "Visit the Cluster Overview for a multi-cluster dashboard with health status, broker details, and topic statistics."
            }
        ]
    };

    public static IReadOnlyList<ShepherdTourDefinition> All { get; } =
    [
        Welcome,
        Topics,
        Consumers,
        Connectors,
        Pipelines,
        Security,
        Monitoring
    ];

    /// <summary>Page-to-tour mapping.</summary>
    private static readonly Dictionary<string, ShepherdTourDefinition> PageMap = new(StringComparer.OrdinalIgnoreCase)
    {
        ["/"] = Welcome,
        ["/topics"] = Topics,
        ["/consumer-groups"] = Consumers,
        ["/connectors"] = Connectors,
        ["/pipelines"] = Pipelines,
        ["/acls"] = Security,
        ["/metrics"] = Monitoring
    };

    public static ShepherdTourDefinition? GetByPage(string page)
    {
        var normalizedPage = page.TrimEnd('/');
        if (string.IsNullOrEmpty(normalizedPage)) normalizedPage = "/";

        return PageMap.GetValueOrDefault(normalizedPage);
    }

    public static ShepherdTourDefinition? GetById(string id)
    {
        return All.FirstOrDefault(t =>
            string.Equals(t.Id, id, StringComparison.OrdinalIgnoreCase));
    }
}
