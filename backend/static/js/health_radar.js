(function () {
    const RADAR_SCRIPT_ID = 'health-radar-script';
    const ECHARTS_SRC = '/static/vendor/echarts.min.js';
    let radarChart = null;
    let radarDataLoaded = false;
    let radarDataLoading = false;

    function injectStyles() {
        if (document.getElementById('health-radar-style')) {
            return;
        }

        const style = document.createElement('style');
        style.id = 'health-radar-style';
        style.textContent = `
            .health-radar-card {
                position: absolute;
                top: calc(100% + 14px);
                right: 0;
                width: 360px;
                background: #ffffff;
                color: #2c3e50;
                border: 1px solid rgba(30, 87, 153, 0.16);
                border-radius: 8px;
                box-shadow: 0 18px 42px rgba(20, 53, 89, 0.26);
                padding: 16px;
                z-index: 9999;
                display: none;
            }

            .health-radar-card::before {
                content: '';
                position: absolute;
                top: -9px;
                right: 28px;
                width: 16px;
                height: 16px;
                background: #ffffff;
                border-left: 1px solid rgba(30, 87, 153, 0.16);
                border-top: 1px solid rgba(30, 87, 153, 0.16);
                transform: rotate(45deg);
            }

            .user-info:hover .health-radar-card,
            .health-radar-card:hover {
                display: block;
            }

            .health-radar-title {
                color: #1e5799;
                font-size: 1rem;
                font-weight: 800;
                margin-bottom: 4px;
            }

            .health-radar-subtitle {
                color: #607486;
                font-size: 0.78rem;
                line-height: 1.5;
                margin-bottom: 10px;
            }

            .health-radar-chart {
                width: 100%;
                height: 260px;
            }

            .health-radar-loading,
            .health-radar-error {
                color: #607486;
                font-size: 0.9rem;
                line-height: 1.6;
                padding: 28px 8px;
                text-align: center;
            }

            .health-radar-error {
                color: #c92a2a;
            }

            .health-radar-details {
                display: grid;
                grid-template-columns: repeat(2, minmax(0, 1fr));
                gap: 6px;
                margin-top: 8px;
            }

            .health-radar-detail {
                color: #425466;
                background: #f3f7fb;
                border-radius: 6px;
                font-size: 0.76rem;
                line-height: 1.35;
                padding: 7px 8px;
            }

            .health-radar-detail strong {
                color: #1e5799;
                display: block;
                margin-bottom: 2px;
            }

            @media (max-width: 760px) {
                .health-radar-card {
                    right: auto;
                    left: 50%;
                    width: min(340px, calc(100vw - 28px));
                    transform: translateX(-50%);
                }

                .health-radar-card::before {
                    right: 50%;
                    transform: translateX(50%) rotate(45deg);
                }
            }
        `;
        document.head.appendChild(style);
    }

    function loadEcharts() {
        if (window.echarts) {
            return Promise.resolve();
        }

        const existing = document.getElementById(RADAR_SCRIPT_ID);
        if (existing) {
            return new Promise((resolve, reject) => {
                existing.addEventListener('load', resolve, { once: true });
                existing.addEventListener('error', reject, { once: true });
            });
        }

        return new Promise((resolve, reject) => {
            const script = document.createElement('script');
            script.id = RADAR_SCRIPT_ID;
            script.src = ECHARTS_SRC;
            script.onload = resolve;
            script.onerror = reject;
            document.head.appendChild(script);
        });
    }

    function createRadarCard(userInfo) {
        let card = userInfo.querySelector('.health-radar-card');
        if (card) {
            return card;
        }

        card = document.createElement('div');
        card.className = 'health-radar-card';
        card.innerHTML = `
            <div class="health-radar-title">健康风险雷达图</div>
            <div class="health-radar-subtitle">基于最近健康工具记录和检测结果生成，分数越高代表当前状态越好。</div>
            <div class="health-radar-loading">正在加载健康画像...</div>
            <div class="health-radar-chart" id="health-radar-chart" style="display:none;"></div>
            <div class="health-radar-details" id="health-radar-details"></div>
        `;
        userInfo.appendChild(card);
        return card;
    }

    function renderDetails(metrics) {
        const detailBox = document.getElementById('health-radar-details');
        if (!detailBox) {
            return;
        }

        detailBox.innerHTML = metrics.map(metric => `
            <div class="health-radar-detail">
                <strong>${metric.label} ${metric.value}</strong>
                <span>${metric.detail}</span>
            </div>
        `).join('');
    }

    function renderRadar(data) {
        const loading = document.querySelector('.health-radar-loading');
        const chartElement = document.getElementById('health-radar-chart');
        if (!chartElement || !data.metrics || !data.metrics.length) {
            if (loading) {
                loading.className = 'health-radar-error';
                loading.textContent = data.message || '暂无可用健康画像数据。';
            }
            return;
        }

        if (loading) {
            loading.style.display = 'none';
        }
        chartElement.style.display = 'block';

        if (!radarChart) {
            radarChart = echarts.init(chartElement);
            window.addEventListener('resize', () => radarChart && radarChart.resize());
        }

        radarChart.setOption({
            tooltip: {
                formatter: params => {
                    const metrics = data.metrics || [];
                    return metrics.map((metric, index) => {
                        const value = params.value[index];
                        return `${metric.label}: ${value}<br/>${metric.detail}`;
                    }).join('<br/><br/>');
                }
            },
            radar: {
                radius: '66%',
                center: ['50%', '52%'],
                indicator: data.metrics.map(metric => ({
                    name: metric.label,
                    max: 100
                })),
                splitNumber: 4,
                axisName: {
                    color: '#425466',
                    fontSize: 12
                },
                splitLine: {
                    lineStyle: {
                        color: ['#dce7f2']
                    }
                },
                splitArea: {
                    areaStyle: {
                        color: ['rgba(41,137,216,0.04)', 'rgba(64,192,87,0.05)']
                    }
                },
                axisLine: {
                    lineStyle: {
                        color: '#c9d6e4'
                    }
                }
            },
            series: [{
                name: '健康状态评分',
                type: 'radar',
                data: [{
                    value: data.metrics.map(metric => metric.value),
                    name: '健康状态评分',
                    areaStyle: {
                        color: 'rgba(41, 137, 216, 0.24)'
                    },
                    lineStyle: {
                        color: '#1e5799',
                        width: 2.5
                    },
                    itemStyle: {
                        color: '#1e5799'
                    }
                }]
            }]
        });

        renderDetails(data.metrics);
        setTimeout(() => radarChart && radarChart.resize(), 30);
    }

    function loadRadarData() {
        if (radarDataLoaded || radarDataLoading) {
            if (radarChart) {
                setTimeout(() => radarChart.resize(), 30);
            }
            return;
        }

        radarDataLoading = true;
        loadEcharts()
            .then(() => fetch('/api/health-risk-radar'))
            .then(response => response.text().then(text => {
                const data = text ? JSON.parse(text) : {};
                if (!response.ok) {
                    throw new Error(data.message || data.error || `请求失败：${response.status}`);
                }
                return data;
            }))
            .then(data => {
                radarDataLoaded = true;
                renderRadar(data);
            })
            .catch(error => {
                const loading = document.querySelector('.health-radar-loading');
                if (loading) {
                    loading.className = 'health-radar-error';
                    loading.textContent = `健康画像加载失败：${error.message}`;
                }
            })
            .finally(() => {
                radarDataLoading = false;
            });
    }

    function initHealthRadar() {
        const userInfo = document.querySelector('.user-info');
        if (!userInfo) {
            return;
        }

        injectStyles();
        createRadarCard(userInfo);
        userInfo.addEventListener('mouseenter', loadRadarData);
        userInfo.addEventListener('focusin', loadRadarData);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initHealthRadar);
    } else {
        initHealthRadar();
    }
})();
