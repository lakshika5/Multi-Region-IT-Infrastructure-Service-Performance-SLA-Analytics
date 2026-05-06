import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pyodbc
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Set style for better visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

print("="*60)
print("EXPLORATORY DATA ANALYSIS - IT INFRASTRUCTURE SLA")
print("="*60)

# ============================================
# 1. CONNECT TO DATABASE AND LOAD DATA
# ============================================
def load_data():
    """Load data from SQL Server for analysis"""
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=localhost\\SQLEXPRESS;'
            'DATABASE=ITInfraSLAAnalytics;'
            'Trusted_Connection=yes;'
        )
        print(" Connected to SQL Server")
        
        # Load ticket data
        query_tickets = """
        SELECT 
            t.ticket_id,
            t.client_id,
            c.client_name,
            c.industry,
            c.region_group,
            p.priority_name,
            t.created_at,
            t.resolved_at,
            t.first_response_at,
            t.agent_interactions,
            t.survey_results,
            DATEDIFF(HOUR, t.created_at, t.resolved_at) as resolution_hours,
            DATEDIFF(HOUR, t.created_at, t.first_response_at) as response_hours,
            CASE 
                WHEN p.priority_name = 'Critical' AND DATEDIFF(HOUR, t.created_at, t.resolved_at) <= 4 THEN 'Met'
                WHEN p.priority_name = 'High' AND DATEDIFF(HOUR, t.created_at, t.resolved_at) <= 8 THEN 'Met'
                WHEN p.priority_name = 'Medium' AND DATEDIFF(HOUR, t.created_at, t.resolved_at) <= 24 THEN 'Met'
                WHEN p.priority_name = 'Low' AND DATEDIFF(HOUR, t.created_at, t.resolved_at) <= 48 THEN 'Met'
                ELSE 'Breached'
            END as sla_status,
            CASE 
                WHEN c.hq_country = 'India' THEN 'India'
                ELSE 'International'
            END as region_type
        FROM fact_ticket t
        JOIN dim_client c ON t.client_id = c.client_id
        JOIN dim_priority p ON t.priority_id = p.priority_id
        WHERE t.resolved_at IS NOT NULL
        """
        
        # Load incident data
        query_incidents = """
        SELECT 
            i.incident_id,
            i.client_id,
            c.client_name,
            c.industry,
            c.region_group,
            s.site_name,
            r.region_name,
            r.country,
            p.priority_name,
            i.opened_at,
            i.resolved_at,
            i.reassignment_count,
            i.reopen_count,
            i.made_sla,
            DATEDIFF(HOUR, i.opened_at, i.resolved_at) as resolution_hours,
            CASE 
                WHEN r.country = 'India' THEN 'India'
                ELSE 'International'
            END as region_type
        FROM fact_incident i
        JOIN dim_client c ON i.client_id = c.client_id
        JOIN dim_site s ON i.site_id = s.site_id
        JOIN dim_region r ON s.region_id = r.region_id
        JOIN dim_priority p ON i.priority_id = p.priority_id
        WHERE i.resolved_at IS NOT NULL
        """
        
        df_tickets = pd.read_sql(query_tickets, conn)
        df_incidents = pd.read_sql(query_incidents, conn)
        
        # Add date features
        for df in [df_tickets, df_incidents]:
            date_col = 'created_at' if 'created_at' in df.columns else 'opened_at'
            df[date_col] = pd.to_datetime(df[date_col])
            df['year'] = df[date_col].dt.year
            df['month'] = df[date_col].dt.month
            df['quarter'] = df[date_col].dt.quarter
            df['day_of_week'] = df[date_col].dt.day_name()
            df['hour_of_day'] = df[date_col].dt.hour
            df['year_month'] = df[date_col].dt.to_period('M')
        
        conn.close()
        print(f" Loaded {len(df_tickets):,} tickets and {len(df_incidents):,} incidents")
        return df_tickets, df_incidents
        
    except Exception as e:
        print(f" Error loading data: {e}")
        return None, None

# Load the data
df_tickets, df_incidents = load_data()

if df_tickets is not None and df_incidents is not None:
    
    # ============================================
    # 2. TICKET VOLUME ANALYSIS
    # ============================================
    print("\n" + "="*60)
    print("2. TICKET VOLUME ANALYSIS")
    print("="*60)
    
    # 2.1 Ticket volume by month
    plt.figure(figsize=(14, 6))
    monthly_tickets = df_tickets.groupby('year_month').size()
    monthly_tickets.plot(kind='line', marker='o', linewidth=2, markersize=8)
    plt.title('Ticket Volume Trend by Month', fontsize=16, fontweight='bold')
    plt.xlabel('Month')
    plt.ylabel('Number of Tickets')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('ticket_volume_monthly.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Monthly Ticket Statistics:")
    print(f"   Average monthly tickets: {monthly_tickets.mean():.0f}")
    print(f"   Peak month: {monthly_tickets.idxmax()} ({monthly_tickets.max()} tickets)")
    print(f"   Lowest month: {monthly_tickets.idxmin()} ({monthly_tickets.min()} tickets)")
    
    # 2.2 Ticket volume by region (India vs International)
    plt.figure(figsize=(10, 6))
    region_volume = df_tickets['region_type'].value_counts()
    colors = ['#ff9999', '#66b3ff']
    plt.pie(region_volume.values, labels=region_volume.index, autopct='%1.1f%%', 
            colors=colors, startangle=90, explode=(0.05, 0))
    plt.title('Ticket Distribution: India vs International', fontsize=16, fontweight='bold')
    plt.tight_layout()
    plt.savefig('ticket_by_region.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Regional Ticket Distribution:")
    for region, count in region_volume.items():
        print(f"   {region}: {count:,} tickets ({count/len(df_tickets)*100:.1f}%)")
    
    # 2.3 Ticket volume by client
    plt.figure(figsize=(14, 6))
    client_volume = df_tickets.groupby('client_name').size().sort_values(ascending=False)
    ax = client_volume.plot(kind='bar', color='skyblue', edgecolor='navy')
    plt.title('Ticket Volume by Client', fontsize=16, fontweight='bold')
    plt.xlabel('Client')
    plt.ylabel('Number of Tickets')
    plt.xticks(rotation=45, ha='right')
    
    # Add value labels on bars
    for i, v in enumerate(client_volume.values):
        ax.text(i, v + 50, str(v), ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('ticket_by_client.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Top Clients by Ticket Volume:")
    for client, count in client_volume.head(3).items():
        print(f"   {client}: {count:,} tickets")
    
    # 2.4 Ticket volume by priority
    plt.figure(figsize=(10, 6))
    priority_volume = df_tickets['priority_name'].value_counts()
    colors = ['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1']
    priority_volume.plot(kind='bar', color=colors, edgecolor='black')
    plt.title('Ticket Volume by Priority', fontsize=16, fontweight='bold')
    plt.xlabel('Priority')
    plt.ylabel('Number of Tickets')
    plt.xticks(rotation=0)
    
    # Add value labels
    for i, v in enumerate(priority_volume.values):
        plt.text(i, v + 100, str(v), ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('ticket_by_priority.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Ticket Distribution by Priority:")
    for priority, count in priority_volume.items():
        pct = count/len(df_tickets)*100
        print(f"   {priority}: {count:,} tickets ({pct:.1f}%)")
    
    # 2.5 Heatmap - Tickets by Day of Week and Hour
    plt.figure(figsize=(14, 8))
    pivot_table = pd.crosstab(df_tickets['day_of_week'], df_tickets['hour_of_day'])
    # Reorder days
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    pivot_table = pivot_table.reindex(day_order)
    
    sns.heatmap(pivot_table, cmap='YlOrRd', annot=True, fmt='d', cbar_kws={'label': 'Ticket Count'})
    plt.title('Ticket Volume Heatmap: Day of Week vs Hour of Day', fontsize=16, fontweight='bold')
    plt.xlabel('Hour of Day')
    plt.ylabel('Day of Week')
    plt.tight_layout()
    plt.savefig('ticket_heatmap.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # ============================================
    # 3. INCIDENT ANALYSIS
    # ============================================
    print("\n" + "="*60)
    print("3. INCIDENT ANALYSIS")
    print("="*60)
    
    # 3.1 Incident counts by region (India vs International)
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    region_incidents = df_incidents['region_type'].value_counts()
    colors = ['#ff9999', '#66b3ff']
    plt.pie(region_incidents.values, labels=region_incidents.index, autopct='%1.1f%%',
            colors=colors, startangle=90, explode=(0.05, 0))
    plt.title('Incidents: India vs International', fontsize=14, fontweight='bold')
    
    plt.subplot(1, 2, 2)
    region_mttr = df_incidents.groupby('region_type')['resolution_hours'].mean()
    region_mttr.plot(kind='bar', color=['#ff9999', '#66b3ff'], edgecolor='black')
    plt.title('Average MTTR by Region', fontsize=14, fontweight='bold')
    plt.xlabel('Region')
    plt.ylabel('Average Resolution Hours')
    plt.xticks(rotation=0)
    
    # Add value labels
    for i, v in enumerate(region_mttr.values):
        plt.text(i, v + 0.5, f'{v:.1f}h', ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('incident_region_analysis.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Incident Regional Analysis:")
    for region, count in region_incidents.items():
        mttr = region_mttr[region]
        print(f"   {region}: {count:,} incidents, Avg MTTR: {mttr:.1f} hours")
    
    # 3.2 Detailed regional breakdown
    plt.figure(figsize=(14, 6))
    region_detail = df_incidents.groupby('region_name').agg({
        'incident_id': 'count',
        'resolution_hours': 'mean',
        'reassignment_count': 'mean',
        'reopen_count': 'mean'
    }).round(2).sort_values('incident_id', ascending=False)
    
    # Plot incidents by specific region
    ax = region_detail['incident_id'].plot(kind='bar', color='lightcoral', edgecolor='darkred')
    plt.title('Incidents by Specific Region', fontsize=16, fontweight='bold')
    plt.xlabel('Region')
    plt.ylabel('Number of Incidents')
    plt.xticks(rotation=45, ha='right')
    
    # Add value labels
    for i, v in enumerate(region_detail['incident_id'].values):
        ax.text(i, v + 20, str(int(v)), ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('incident_by_region_detail.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Detailed Regional Incident Stats:")
    print(region_detail.to_string())
    
    # 3.3 MTTR comparison box plot
    plt.figure(figsize=(12, 6))
    df_incidents_box = df_incidents[df_incidents['resolution_hours'] < 100]  # Remove outliers for better viz
    
    sns.boxplot(data=df_incidents_box, x='region_type', y='resolution_hours', palette=['#ff9999', '#66b3ff'])
    plt.title('MTTR Distribution: India vs International', fontsize=16, fontweight='bold')
    plt.xlabel('Region')
    plt.ylabel('Resolution Hours')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig('mttr_boxplot.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # ============================================
    # 4. REOPEN / REASSIGNMENT PATTERNS
    # ============================================
    print("\n" + "="*60)
    print("4. REOPEN / REASSIGNMENT PATTERNS")
    print("="*60)
    
    # 4.1 Overall reassignment and reopen stats
    print("\n Overall Reassignment & Reopen Statistics:")
    print(f"   Average reassignments per incident: {df_incidents['reassignment_count'].mean():.2f}")
    print(f"   Incidents with reassignments: {(df_incidents['reassignment_count'] > 0).sum():,} ({(df_incidents['reassignment_count'] > 0).mean()*100:.1f}%)")
    print(f"   Average reopens per incident: {df_incidents['reopen_count'].mean():.2f}")
    print(f"   Incidents with reopens: {(df_incidents['reopen_count'] > 0).sum():,} ({(df_incidents['reopen_count'] > 0).mean()*100:.1f}%)")
    
    # 4.2 Reassignment rate by client
    plt.figure(figsize=(14, 6))
    client_reassign = df_incidents.groupby('client_name').agg({
        'incident_id': 'count',
        'reassignment_count': 'mean',
        'reopen_count': 'mean'
    }).round(2).sort_values('reassignment_count', ascending=False)
    
    x = range(len(client_reassign))
    width = 0.35
    
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.bar([i - width/2 for i in x], client_reassign['reassignment_count'], width, 
        label='Avg Reassignments', color='lightcoral', edgecolor='darkred')
    ax.bar([i + width/2 for i in x], client_reassign['reopen_count'], width, 
        label='Avg Reopens', color='lightblue', edgecolor='navy')
    
    ax.set_xlabel('Client')
    ax.set_ylabel('Average Count')
    ax.set_title('Reassignment and Reopen Rates by Client', fontsize=16, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(client_reassign.index, rotation=45, ha='right')
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('reassign_reopen_by_client.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Top 3 Clients by Reassignment Rate:")
    for client, row in client_reassign.head(3).iterrows():
        print(f"   {client}: {row['reassignment_count']:.2f} avg reassignments, {row['reopen_count']:.2f} avg reopens ({int(row['incident_id'])} incidents)")
    
    # 4.3 Reassignment rate by region
    plt.figure(figsize=(12, 5))
    
    plt.subplot(1, 2, 1)
    region_reassign = df_incidents.groupby('region_type')['reassignment_count'].mean()
    region_reassign.plot(kind='bar', color=['#ff9999', '#66b3ff'], edgecolor='black')
    plt.title('Avg Reassignments by Region', fontsize=14, fontweight='bold')
    plt.xlabel('Region')
    plt.ylabel('Average Reassignments')
    plt.xticks(rotation=0)
    
    for i, v in enumerate(region_reassign.values):
        plt.text(i, v + 0.02, f'{v:.2f}', ha='center', fontweight='bold')
    
    plt.subplot(1, 2, 2)
    region_reopen = df_incidents.groupby('region_type')['reopen_count'].mean()
    region_reopen.plot(kind='bar', color=['#ff9999', '#66b3ff'], edgecolor='black')
    plt.title('Avg Reopens by Region', fontsize=14, fontweight='bold')
    plt.xlabel('Region')
    plt.ylabel('Average Reopens')
    plt.xticks(rotation=0)
    
    for i, v in enumerate(region_reopen.values):
        plt.text(i, v + 0.01, f'{v:.2f}', ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('reassign_reopen_by_region.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # 4.4 Correlation between reassignments and resolution time
    plt.figure(figsize=(10, 6))
    
    # Create bins for reassignment count
    df_incidents['reassign_bin'] = pd.cut(df_incidents['reassignment_count'], 
                                        bins=[-1, 0, 1, 2, 3, 100], 
                                        labels=['0', '1', '2', '3', '4+'])
    
    reassign_impact = df_incidents.groupby('reassign_bin')['resolution_hours'].agg(['mean', 'median', 'count'])
    
    ax = reassign_impact['mean'].plot(kind='bar', color='lightcoral', edgecolor='darkred')
    plt.title('Impact of Reassignments on Resolution Time', fontsize=16, fontweight='bold')
    plt.xlabel('Number of Reassignments')
    plt.ylabel('Average Resolution Hours')
    plt.xticks(rotation=0)
    
    # Add value labels
    for i, v in enumerate(reassign_impact['mean'].values):
        ax.text(i, v + 1, f'{v:.1f}h\n(n={int(reassign_impact["count"].values[i])})', 
                ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('reassign_impact.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n Impact of Reassignments on Resolution Time:")
    for bin_name, row in reassign_impact.iterrows():
        print(f"   {bin_name} reassignments: {row['mean']:.1f} hours avg ({int(row['count'])} incidents)")
    
    # ============================================
    # 5. CORRELATION ANALYSIS
    # ============================================
    print("\n" + "="*60)
    print("5. CORRELATION ANALYSIS")
    print("="*60)
    
    # 5.1 Correlation matrix for numerical features
    numeric_cols = ['resolution_hours', 'reassignment_count', 'reopen_count']
    if 'response_hours' in df_tickets.columns:
        ticket_numeric = df_tickets[['resolution_hours', 'response_hours', 'agent_interactions']].select_dtypes(include=[np.number])
    else:
        ticket_numeric = df_tickets[['resolution_hours', 'agent_interactions']].select_dtypes(include=[np.number])
    
    incident_numeric = df_incidents[numeric_cols].select_dtypes(include=[np.number])
    
    plt.figure(figsize=(14, 5))
    
    plt.subplot(1, 2, 1)
    if len(ticket_numeric.columns) > 1:
        corr_tickets = ticket_numeric.corr()
        sns.heatmap(corr_tickets, annot=True, cmap='coolwarm', center=0, 
                    square=True, linewidths=1, cbar_kws={'label': 'Correlation'})
        plt.title('Ticket Metrics Correlation', fontsize=14, fontweight='bold')
    
    plt.subplot(1, 2, 2)
    if len(incident_numeric.columns) > 1:
        corr_incidents = incident_numeric.corr()
        sns.heatmap(corr_incidents, annot=True, cmap='coolwarm', center=0,
                    square=True, linewidths=1, cbar_kws={'label': 'Correlation'})
        plt.title('Incident Metrics Correlation', fontsize=14, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('correlation_matrix.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    # 5.2 Priority vs Resolution Time
    plt.figure(figsize=(12, 6))
    priority_mttr = df_incidents.groupby('priority_name')['resolution_hours'].agg(['mean', 'median', 'count']).round(2)
    
    ax = priority_mttr['mean'].plot(kind='bar', color=['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1'], edgecolor='black')
    plt.title('Average Resolution Time by Priority', fontsize=16, fontweight='bold')
    plt.xlabel('Priority')
    plt.ylabel('Average Resolution Hours')
    plt.xticks(rotation=0)
    
    # Add value labels
    for i, (idx, row) in enumerate(priority_mttr.iterrows()):
        ax.text(i, row['mean'] + 1, f'{row["mean"]:.1f}h\n(n={int(row["count"])})', 
                ha='center', fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('mttr_by_priority.png', dpi=300, bbox_inches='tight')
    plt.show()
    
    print("\n⚡ Resolution Time by Priority:")
    print(priority_mttr.to_string())
    
    # ============================================
    # 6. SUMMARY STATISTICS
    # ============================================
    print("\n" + "="*60)
    print("6. SUMMARY STATISTICS")
    print("="*60)
    
    print("\n TICKETS SUMMARY:")
    print(f"   Total Tickets: {len(df_tickets):,}")
    print(f"   Avg Resolution Time: {df_tickets['resolution_hours'].mean():.1f} hours")
    print(f"   Median Resolution Time: {df_tickets['resolution_hours'].median():.1f} hours")
    print(f"   SLA Compliance Rate: {(df_tickets['sla_status'] == 'Met').mean()*100:.1f}%")
    print(f"   CSAT Distribution:")
    print(df_tickets['survey_results'].value_counts(normalize=True).mul(100).round(1).to_string())
    
    print("\n INCIDENTS SUMMARY:")
    print(f"   Total Incidents: {len(df_incidents):,}")
    print(f"   Avg Resolution Time: {df_incidents['resolution_hours'].mean():.1f} hours")
    print(f"   Median Resolution Time: {df_incidents['resolution_hours'].median():.1f} hours")
    print(f"   SLA Compliance Rate: {df_incidents['made_sla'].mean()*100:.1f}%")
    print(f"   Avg Reassignments: {df_incidents['reassignment_count'].mean():.2f}")
    print(f"   Avg Reopens: {df_incidents['reopen_count'].mean():.2f}")
    
    # ============================================
    # 7. KEY INSIGHTS SUMMARY
    # ============================================
    print("\n" + "="*60)
    print("7. KEY INSIGHTS FOR PROJECT REPORT")
    print("="*60)
    
    insights = [
        "1. TICKET VOLUME: Peak in [month], lowest in [month] - suggests seasonal patterns",
        f"2. REGIONAL DISTRIBUTION: {region_volume.get('India', 0)/len(df_tickets)*100:.1f}% of tickets from India, {region_volume.get('International', 0)/len(df_tickets)*100:.1f}% International",
        f"3. PRIORITY DISTRIBUTION: {priority_volume.get('Medium', 0)/len(df_tickets)*100:.1f}% Medium priority tickets - largest category",
        f"4. INCIDENT MTTR: International incidents take {region_mttr.get('International', 0)-region_mttr.get('India', 0):.1f} hours longer to resolve than India",
        f"5. REASSIGNMENT IMPACT: Each reassignment adds ~{reassign_impact['mean'].diff().mean():.1f} hours to resolution time",
        f"6. CLIENT HEALTH: {client_reassign.index[0]} has highest reassignment rate ({client_reassign.iloc[0]['reassignment_count']:.2f} per incident)",
        f"7. CRITICAL INCIDENTS: Take {priority_mttr.loc['Critical', 'mean']:.1f} hours on average to resolve",
        "8. TIME PATTERNS: Most tickets created during business hours (9 AM - 5 PM)",
        "9. REOPEN RATE: {:.1f}% of incidents are reopened".format((df_incidents['reopen_count'] > 0).mean()*100),
        "10. RECOMMENDATION: Focus on reducing reassignments in {} region to improve MTTR".format(
            'International' if region_reassign['International'] > region_reassign['India'] else 'India'
        )
    ]
    
    for insight in insights:
        print(insight)
    
    print("\n" + "="*60)
    print(" EXPLORATORY ANALYSIS COMPLETE")
    
else:
    print(" Failed to load data. Please check database connection.")