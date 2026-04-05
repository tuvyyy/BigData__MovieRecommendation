import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type MouseEvent as ReactMouseEvent,
} from 'react'
import { AnimatePresence, motion, useReducedMotion, useScroll, useTransform } from 'framer-motion'
import { ArrowUpRight, ChevronDown, Sparkles } from 'lucide-react'
import './App.css'

type SectionId =
  | 'home'
  | 'services'
  | 'featured'
  | 'works'
  | 'awards'
  | 'about'
  | 'clients'
  | 'contact'

type NavItem = {
  id: SectionId
  label: string
}
type Locale = 'en' | 'vi'

type ServiceItem = {
  index: string
  title: string
  description: string
  image: string
}

type WorkItem = {
  title: string
  category: string
  year: string
  description: string
  image: string
  dominant?: boolean
}

type AwardItem = {
  tag: string
  title: string
  description: string
  year: string
}

type Burst = {
  id: number
  x: number
  y: number
}

type RecommendationRow = {
  movieId: number
  title: string
  genres: string
  score: number
  als_score?: number | null
  content_score?: number | null
  rank?: number | null
  explain?: string
}

type RecommendationPayload = {
  route?: string
  user_id?: number
  recommendations?: RecommendationRow[]
}

type AuthUser = {
  id_nguoi_dung: number
  ten_tai_khoan: string
  email: string
  ho_ten?: string | null
  vai_tro?: string
}

type UserProfile = {
  id_ho_so: number
  id_nguoi_dung: number
  ten_ho_so: string
  che_do_goi_y: string
  id_user_ml: number
  the_loai_uu_tien?: string | null
}

type UserMode = 'existing' | 'new'
type RecoStage = 'onboard' | 'discover' | 'feedback'
type PageMode = 'landing' | 'recommend'

const SECTION_IDS: readonly SectionId[] = [
  'home',
  'services',
  'featured',
  'works',
  'awards',
  'about',
  'clients',
  'contact',
]

const NAV_ITEMS_EN: readonly NavItem[] = [
  { id: 'home', label: 'Home' },
  { id: 'services', label: 'Modules' },
  { id: 'featured', label: 'Demo Flow' },
  { id: 'works', label: 'Scenarios' },
  { id: 'awards', label: 'Metrics' },
  { id: 'about', label: 'Architecture' },
  { id: 'clients', label: 'Stack' },
  { id: 'contact', label: 'Run' },
]
const NAV_ITEMS_VI: readonly NavItem[] = [
  { id: 'home', label: 'Trang chủ' },
  { id: 'services', label: 'Mô-đun' },
  { id: 'featured', label: 'Luồng demo' },
  { id: 'works', label: 'Kịch bản' },
  { id: 'awards', label: 'Chỉ số' },
  { id: 'about', label: 'Kiến trúc' },
  { id: 'clients', label: 'Công nghệ' },
  { id: 'contact', label: 'Vận hành' },
]

const HERO_THUMBNAILS = [
  {
    title: 'Shawshank Redemption',
    image:
      'https://images.unsplash.com/photo-1478720568477-152d9b164e26?auto=format&fit=crop&w=800&q=80',
    score: 98,
  },
  {
    title: 'Godfather',
    image:
      'https://images.unsplash.com/photo-1517604931442-7e0c8ed2963c?auto=format&fit=crop&w=800&q=80',
    score: 96,
  },
  {
    title: 'Star Wars IV',
    image:
      'https://images.unsplash.com/photo-1524985069026-dd778a71c7b4?auto=format&fit=crop&w=800&q=80',
    score: 94,
  },
  {
    title: 'Silence of the Lambs',
    image:
      'https://images.unsplash.com/photo-1536440136628-849c177e76a1?auto=format&fit=crop&w=800&q=80',
    score: 93,
  },
]

const STATS_EN = [
  { label: 'Ratings Ingested', value: '1M+' },
  { label: 'User Buckets', value: '64' },
  { label: 'Serving Route', value: 'ALS + Content' },
  { label: 'Top-N API', value: 'Live' },
]
const STATS_VI = [
  { label: 'Lượt đánh giá đã nạp', value: '1M+' },
  { label: 'Số user bucket', value: '64' },
  { label: 'Tuyến phục vụ', value: 'ALS + Content' },
  { label: 'API Top-N', value: 'Đang chạy' },
]

const SERVICES_EN: readonly ServiceItem[] = [
  {
    index: '01',
    title: 'Distributed ETL Layer',
    description:
      'Ingest ratings and movies with explicit schemas, validate quality, partition by user bucket, and persist optimized Parquet for scalable training.',
    image:
      'https://images.unsplash.com/photo-1461749280684-dccba630e2f6?auto=format&fit=crop&w=960&q=80',
  },
  {
    index: '02',
    title: 'Hybrid Re-ranking Engine',
    description:
      'Generate ALS candidates, blend with genre-profile signals, and rerank with diversity controls so recommendations feel both personal and fresh.',
    image:
      'https://images.unsplash.com/photo-1489599904767-a59d8d4d4d0e?auto=format&fit=crop&w=960&q=80',
  },
  {
    index: '03',
    title: 'Cold Start Router',
    description:
      'Route new users to fallback Top-N global or genre-based lists while returning users receive personalized hybrid recommendations.',
    image:
      'https://images.unsplash.com/photo-1485846234645-a62644f84728?auto=format&fit=crop&w=960&q=80',
  },
  {
    index: '04',
    title: 'Feedback & Retrain Loop',
    description:
      'Capture rating events from API/UI, append behavior logs, retrain by version timestamp, and redeploy precomputed recommendations.',
    image:
      'https://images.unsplash.com/photo-1518770660439-4636190af475?auto=format&fit=crop&w=960&q=80',
  },
]
const SERVICES_VI: readonly ServiceItem[] = [
  {
    index: '01',
    title: 'Tầng ETL phân tán',
    description:
      'Nạp ratings và movies bằng schema rõ ràng, làm sạch dữ liệu, partition theo user bucket và lưu Parquet tối ưu cho huấn luyện lớn.',
    image:
      'https://images.unsplash.com/photo-1461749280684-dccba630e2f6?auto=format&fit=crop&w=960&q=80',
  },
  {
    index: '02',
    title: 'Hybrid Re-ranking',
    description:
      'Sinh ứng viên từ ALS, trộn với tín hiệu thể loại, sắp xếp lại theo đa dạng để kết quả vừa đúng gu vừa không bị lặp.',
    image:
      'https://images.unsplash.com/photo-1489599904767-a59d8d4d4d0e?auto=format&fit=crop&w=960&q=80',
  },
  {
    index: '03',
    title: 'Cold Start Router',
    description:
      'User mới sẽ đi vào fallback Top-N toàn cục hoặc theo thể loại; user cũ sẽ nhận gợi ý cá nhân hóa tuyến hybrid.',
    image:
      'https://images.unsplash.com/photo-1485846234645-a62644f84728?auto=format&fit=crop&w=960&q=80',
  },
  {
    index: '04',
    title: 'Feedback và Retrain',
    description:
      'Nhận sự kiện đánh giá từ API/UI, append logs hành vi, retrain theo phiên bản timestamp và cập nhật kết quả precompute.',
    image:
      'https://images.unsplash.com/photo-1518770660439-4636190af475?auto=format&fit=crop&w=960&q=80',
  },
]

const WORKS_EN: readonly WorkItem[] = [
  {
    title: 'Returning User Session',
    category: 'ALS + Hybrid Route',
    year: 'User #2',
    description:
      'Loads precomputed top-50 candidates, reranks by genre affinity, and returns explainable top-10 in one response.',
    image:
      'https://images.unsplash.com/photo-1517604931442-7e0c8ed2963c?auto=format&fit=crop&w=1200&q=80',
    dominant: true,
  },
  {
    title: 'New User Fallback',
    category: 'Cold Start Route',
    year: 'User #999999',
    description: 'No history detected, system switches to popularity + genre fallback and returns safe high-confidence picks.',
    image:
      'https://images.unsplash.com/photo-1536440136628-849c177e76a1?auto=format&fit=crop&w=1200&q=80',
  },
  {
    title: 'Genre-First Discovery',
    category: 'Content Profile',
    year: 'Top-N by Genre',
    description: 'Ranks titles aligned with favorite genres from user history to stabilize quality under sparse interactions.',
    image:
      'https://images.unsplash.com/photo-1524985069026-dd778a71c7b4?auto=format&fit=crop&w=1200&q=80',
  },
  {
    title: 'Explainable API Payload',
    category: 'Serving Layer',
    year: 'FastAPI',
    description: 'Every recommendation includes route source and rationale such as similar users or favorite genre match.',
    image:
      'https://images.unsplash.com/photo-1478720568477-152d9b164e26?auto=format&fit=crop&w=1200&q=80',
  },
  {
    title: 'Feedback Retrain Cycle',
    category: 'Batch MLOps',
    year: 'Versioned',
    description: 'New ratings append to logs, training reruns with fixed seed, and the model snapshot is versioned by timestamp.',
    image:
      'https://images.unsplash.com/photo-1517430816045-df4b7de11d1d?auto=format&fit=crop&w=1200&q=80',
  },
]
const WORKS_VI: readonly WorkItem[] = [
  {
    title: 'Phiên user cũ',
    category: 'Tuyến ALS + Hybrid',
    year: 'User #2',
    description:
      'Tải candidate top-50 đã precompute, rerank theo gu thể loại, trả về top-10 kèm giải thích trong một lần gọi.',
    image:
      'https://images.unsplash.com/photo-1517604931442-7e0c8ed2963c?auto=format&fit=crop&w=1200&q=80',
    dominant: true,
  },
  {
    title: 'Fallback cho user mới',
    category: 'Cold Start Route',
    year: 'User #999999',
    description:
      'Không có lịch sử đánh giá, hệ thống chuyển sang fallback phổ biến + thể loại để đảm bảo kết quả an toàn.',
    image:
      'https://images.unsplash.com/photo-1536440136628-849c177e76a1?auto=format&fit=crop&w=1200&q=80',
  },
  {
    title: 'Khám phá theo thể loại',
    category: 'Content Profile',
    year: 'Top-N theo thể loại',
    description:
      'Xếp phim theo nhóm thể loại yêu thích từ lịch sử user để ổn định chất lượng trong trường hợp sparse.',
    image:
      'https://images.unsplash.com/photo-1524985069026-dd778a71c7b4?auto=format&fit=crop&w=1200&q=80',
  },
  {
    title: 'Payload có giải thích',
    category: 'Serving Layer',
    year: 'FastAPI',
    description:
      'Mỗi gợi ý đều kèm route, score và lý do để phục vụ demo, kiểm chứng và phân tích hệ thống.',
    image:
      'https://images.unsplash.com/photo-1478720568477-152d9b164e26?auto=format&fit=crop&w=1200&q=80',
  },
  {
    title: 'Vòng feedback retrain',
    category: 'Batch MLOps',
    year: 'Versioned',
    description:
      'Đánh giá mới được append vào logs, model được retrain với seed cố định, snapshot mới lưu theo timestamp.',
    image:
      'https://images.unsplash.com/photo-1517430816045-df4b7de11d1d?auto=format&fit=crop&w=1200&q=80',
  },
]

const AWARDS_EN: readonly AwardItem[] = [
  {
    tag: 'Offline Metric',
    title: 'RMSE Leaderboard',
    description: 'Track hyperparameter runs for rank, regParam, and maxIter. Persist best model and validation evidence.',
    year: 'Latest Run',
  },
  {
    tag: 'Ranking Metric',
    title: 'Precision@K',
    description: 'Measure recommendation relevance in top slots to validate what users actually see first.',
    year: 'K=10',
  },
  {
    tag: 'Ranking Metric',
    title: 'Recall@K',
    description: 'Monitor how much relevant inventory is recovered in recommendation lists for each user segment.',
    year: 'K=10',
  },
  {
    tag: 'Ranking Metric',
    title: 'NDCG@K',
    description: 'Reward correct ordering so high-value movies surface first, not just anywhere in the list.',
    year: 'K=10',
  },
]
const AWARDS_VI: readonly AwardItem[] = [
  {
    tag: 'Offline Metric',
    title: 'Bảng RMSE',
    description:
      'Theo dõi các lần tune rank, regParam, maxIter; chọn best model và lưu bằng chứng trong metrics.',
    year: 'Lần chạy mới nhất',
  },
  {
    tag: 'Ranking Metric',
    title: 'Precision@K',
    description:
      'Đo mức độ liên quan của kết quả ở các vị trí đầu danh sách để đánh giá trải nghiệm thực tế của người dùng.',
    year: 'K=10',
  },
  {
    tag: 'Ranking Metric',
    title: 'Recall@K',
    description:
      'Đo tỷ lệ item liên quan được thu hồi trong top-k, theo từng nhóm user và từng chế độ gợi ý.',
    year: 'K=10',
  },
  {
    tag: 'Ranking Metric',
    title: 'NDCG@K',
    description:
      'Thưởng cho thứ tự xếp hạng đúng, đảm bảo phim giá trị cao được đưa lên vị trí ưu tiên.',
    year: 'K=10',
  },
]

const CLIENTS = [
  'PySpark',
  'Spark SQL',
  'ALS MLlib',
  'FastAPI',
  'Streamlit',
  'Parquet',
  'MovieLens',
  'Redis (Optional)',
]
const EASE_SMOOTH: [number, number, number, number] = [0.22, 1, 0.36, 1]
const EASE_SLAM: [number, number, number, number] = [0.2, 1, 0.36, 1]
const TEXT = {
  en: {
    brand: 'KHANGDAUTI',
    signIn: 'Sign in',
    signUp: 'Sign up',
    heroKicker: 'PySpark + ALS + Hybrid + Cold Start + Feedback',
    heroTitle: 'STOP SCROLLING, START WATCHING.',
    heroCopy:
      'Production-grade movie recommendation platform on MovieLens: distributed ETL, optimized Spark training, hybrid scoring, cold-start routing, explainable API results, and retraining loop.',
    heroPrimaryCta: 'Open recommendations',
    heroSecondaryCta: 'Explore system modules',
    plateModules: 'SYSTEM MODULES',
    plateFlow: 'RECOMMENDATION FLOW',
    plateScenarios: 'DISCOVERY SCENARIOS',
    plateMetrics: 'METRICS EVIDENCE',
    servicesKicker: 'System Layers',
    servicesTitle: 'RECOMMENDATION MODULES',
    featuredKicker: 'Featured Demo',
    featuredTitle: 'RETURNING USER / HYBRID ROUTE',
    featuredInputLabel: 'Input',
    featuredRouteLabel: 'Route',
    featuredOutputLabel: 'Output',
    featuredExplainLabel: 'Explainability',
    featuredInputValue: 'User #2',
    featuredRouteValue: 'ALS + Content',
    featuredOutputValue: 'Top-10 Ranked',
    featuredExplainValue: 'Reason + Score',
    worksKicker: 'Discovery Modes',
    worksTitle: 'USER SCENARIOS',
    awardsKicker: 'Evaluation',
    awardsTitle: 'METRICS & EVIDENCE',
    awardsCopy:
      'This system is scored like a real recommender stack: loss metrics for model fit and ranking metrics for what users actually see on screen.',
    aboutTitle: 'SCALABLE MOVIE RECOMMENDATION, BUILT FOR BIG DATA.',
    aboutCopy:
      'The architecture separates offline compute from online serving: ETL and training run in Spark, recommendations are precomputed to gold storage, and API responses stay fast and stable for demo.',
    aboutHighlights: [
      'AQE + Tuned Shuffle',
      'Broadcast + Cache Optimizations',
      'Cold Start Fallback Router',
      'Feedback-Driven Retraining',
    ],
    clientsKicker: 'Platform Stack',
    contactKicker: 'Contact',
    contactTitle: 'READY FOR DEMO DAY: RUN PIPELINE, OPEN API, START UI.',
    contactCopy:
      'Execute ETL + training pipeline, serve recommendation endpoints, and validate old/new user scenarios with explainable outputs directly from the interface.',
    contactEmail: 'mlops@movie-reco.local',
    contactButton: 'Run health check',
    footerLeft: 'KHANGDAUTI',
    footerMid: 'Hybrid Spark Recommender',
    footerRight: '(c) 2026 All rights reserved',
  },
  vi: {
    brand: 'KHANGDAUTI',
    signIn: 'Đăng nhập',
    signUp: 'Đăng ký',
    heroKicker: 'PySpark + ALS + Hybrid + Cold Start + Feedback',
    heroTitle: 'DỪNG LƯỚT, BẬT PHIM NGAY.',
    heroCopy:
      'Nền tảng gợi ý phim trên MovieLens theo hướng production: ETL phân tán, huấn luyện Spark tối ưu, hybrid ranking, cold-start routing, API có giải thích và vòng retrain.',
    heroPrimaryCta: 'Mở gợi ý cá nhân hóa',
    heroSecondaryCta: 'Xem các mô-đun hệ thống',
    plateModules: 'MÔ-ĐUN HỆ THỐNG',
    plateFlow: 'LUỒNG GỢI Ý PHIM',
    plateScenarios: 'KỊCH BẢN NGƯỜI DÙNG',
    plateMetrics: 'CHỈ SỐ ĐÁNH GIÁ',
    servicesKicker: 'Tầng hệ thống',
    servicesTitle: 'CÁC MÔ-ĐUN GỢI Ý',
    featuredKicker: 'Demo nổi bật',
    featuredTitle: 'USER CŨ / TUYẾN HYBRID',
    featuredInputLabel: 'Đầu vào',
    featuredRouteLabel: 'Tuyến',
    featuredOutputLabel: 'Đầu ra',
    featuredExplainLabel: 'Giải thích',
    featuredInputValue: 'User #2',
    featuredRouteValue: 'ALS + Content',
    featuredOutputValue: 'Top-10 đã xếp',
    featuredExplainValue: 'Lý do + Score',
    worksKicker: 'Chế độ khám phá',
    worksTitle: 'KỊCH BẢN GỢI Ý',
    awardsKicker: 'Đánh giá',
    awardsTitle: 'METRICS & BẰNG CHỨNG',
    awardsCopy:
      'Hệ thống được đánh giá như recommender thực tế: RMSE cho độ khớp model và ranking metrics cho chất lượng kết quả top-k.',
    aboutTitle: 'HỆ THỐNG GỢI Ý PHIM MỞ RỘNG CHO BIG DATA.',
    aboutCopy:
      'Kiến trúc tách offline và online: ETL + train chạy trên Spark, kết quả precompute ở gold layer, API phục vụ nhanh và ổn định cho demo.',
    aboutHighlights: [
      'AQE + Shuffle được tune',
      'Broadcast + Cache tối ưu',
      'Cold Start Fallback Router',
      'Feedback retrain theo batch',
    ],
    clientsKicker: 'Công nghệ nền',
    contactKicker: 'Vận hành',
    contactTitle: 'SẴN SÀNG DEMO: CHẠY PIPELINE, BẬT API, MỞ UI.',
    contactCopy:
      'Chạy ETL + training, khởi động endpoint gợi ý, và kiểm thử 2 tình huống user cũ / user mới với kết quả có giải thích.',
    contactEmail: 'mlops@movie-reco.local',
    contactButton: 'Kiểm tra health',
    footerLeft: 'KHANGDAUTI',
    footerMid: 'Hybrid Spark Recommender',
    footerRight: '(c) 2026 All rights reserved',
  },
} as const

function TransitionPlate({ label }: { label: string }) {
  return (
    <motion.div
      className="transition-plate"
      initial={{ opacity: 0, y: 20 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ amount: 0.5, once: true }}
      transition={{ duration: 0.65, ease: EASE_SMOOTH }}
    >
      <span className="transition-plate-noise" aria-hidden />
      <span className="transition-plate-vignette" aria-hidden />
      <motion.p
        className="transition-plate-label"
        initial={{ opacity: 0, scale: 0.985 }}
        whileInView={{ opacity: 1, scale: 1 }}
        viewport={{ amount: 0.6, once: true }}
        transition={{ duration: 0.52, delay: 0.08, ease: EASE_SMOOTH }}
      >
        {label}
      </motion.p>
    </motion.div>
  )
}

type RecommendationHeaderProps = {
  locale: Locale
  stageTabs: ReadonlyArray<{ key: RecoStage; label: string }>
  recoStage: RecoStage
  setRecoStage: (stage: RecoStage) => void
  apiHealth: string
  routeBadge: string
  routeExplain: string
}

function RecommendationHeader({
  locale,
  stageTabs,
  recoStage,
  setRecoStage,
  apiHealth,
  routeBadge,
  routeExplain,
}: RecommendationHeaderProps) {
  const activeIndex = stageTabs.findIndex((tab) => tab.key === recoStage)
  return (
    <div className="recommend-shell-head">
      <div className="recommend-panel-head">
        <div className="recommend-stage-tabs">
          {stageTabs.map((tab, index) => {
            const stateClass =
              index < activeIndex ? 'is-complete' : index === activeIndex ? 'is-active' : 'is-inactive'

            return (
              <button
                key={tab.key}
                type="button"
                className={`recommend-stage-btn ${stateClass}`}
                onClick={() => setRecoStage(tab.key)}
              >
                <span className="recommend-stage-num">{index + 1}</span>
                <span>{tab.label}</span>
              </button>
            )
          })}
        </div>
        <div className="recommend-status-line">
          <span className={`recommend-health-dot ${apiHealth === 'online' ? 'is-online' : 'is-offline'}`} />
          <p>
            {apiHealth === 'online'
              ? locale === 'vi'
                ? 'Hệ thống sẵn sàng'
                : 'System ready'
              : locale === 'vi'
                ? 'Tạm thời chưa kết nối được hệ thống'
                : 'System is temporarily unreachable'}
          </p>
        </div>
      </div>
      <div className="recommend-route-wrap">
        <span className="recommend-route-badge">{routeBadge}</span>
        <p className="recommend-route-note">{routeExplain}</p>
      </div>
    </div>
  )
}

type UserModeSelectorProps = {
  locale: Locale
  userMode: UserMode
  onSelectMode: (mode: UserMode) => void
}

function UserModeSelector({ locale, userMode, onSelectMode }: UserModeSelectorProps) {
  return (
    <div className="recommend-profiles">
      <button
        type="button"
        className={`recommend-profile-btn ${userMode === 'existing' ? 'is-active' : ''}`}
        onClick={() => onSelectMode('existing')}
      >
        <strong>
          {locale === 'vi' ? 'Gợi ý cá nhân hóa' : 'Personalized recommendations'}
        </strong>
        <span>
          {locale === 'vi'
            ? 'Dành cho người đã có lịch sử xem và đánh giá (ALS).'
            : 'Prioritizes personalized recommendations from past behavior (ALS).'}
        </span>
      </button>
      <button
        type="button"
        className={`recommend-profile-btn ${userMode === 'new' ? 'is-active' : ''}`}
        onClick={() => onSelectMode('new')}
      >
        <strong>
          {locale === 'vi' ? 'Gợi ý theo sở thích ban đầu' : 'Starter preference recommendations'}
        </strong>
        <span>
          {locale === 'vi'
            ? 'Dành cho hồ sơ mới, ưu tiên thể loại và độ phổ biến (Cold Start).'
            : 'Uses genre + popularity before enough data is available (Cold Start).'}
        </span>
      </button>
    </div>
  )
}

type FilterBarProps = {
  locale: Locale
  isAuthenticated: boolean
  authUser: AuthUser | null
  profiles: UserProfile[]
  activeProfileId: number | null
  setActiveProfileId: (id: number) => void
  recoTopN: number
  setRecoTopN: (value: number) => void
  recoGenre: string
  setRecoGenre: (genre: string) => void
  quickGenres: string[]
  recoLoading: boolean
  ratedCount: number
  onFetchRecommendations: () => void
  onOpenFeedback: () => void
  onOpenAuth: () => void
  onBurst: (event: ReactMouseEvent<HTMLElement>) => void
}

function FilterBar({
  locale,
  isAuthenticated,
  authUser,
  profiles,
  activeProfileId,
  setActiveProfileId,
  recoTopN,
  setRecoTopN,
  recoGenre,
  setRecoGenre,
  quickGenres,
  recoLoading,
  ratedCount,
  onFetchRecommendations,
  onOpenFeedback,
  onOpenAuth,
  onBurst,
}: FilterBarProps) {
  const activeProfile = profiles.find((profile) => profile.id_ho_so === activeProfileId) ?? null
  const profileModeLabel = activeProfile?.che_do_goi_y === 'ca_nhan_hoa'
    ? locale === 'vi'
      ? 'Cá nhân hóa (ALS)'
      : 'Personalized (ALS)'
    : locale === 'vi'
      ? 'Sở thích ban đầu (Cold Start)'
      : 'Starter preferences (Cold Start)'

  return (
    <div className="recommend-filter-card">
      <div className="recommend-form">
        <label>
          <span>{locale === 'vi' ? 'Hồ sơ hiện tại' : 'Current profile'}</span>
          {isAuthenticated ? (
            <select
              className="recommend-input"
              value={activeProfileId ?? ''}
              onChange={(event) => setActiveProfileId(Number(event.target.value))}
            >
              {profiles.map((profile) => (
                <option key={profile.id_ho_so} value={profile.id_ho_so}>
                  {profile.ten_ho_so}
                </option>
              ))}
            </select>
          ) : (
            <div className="recommend-auth-inline">
              <span className="recommend-auth-note">
                {locale === 'vi' ? 'Đăng nhập để sử dụng hồ sơ cá nhân hóa.' : 'Sign in to use personalized profiles.'}
              </span>
              <button
                type="button"
                className="recommend-chip-btn"
                onMouseDown={onBurst}
                onClick={onOpenAuth}
              >
                {locale === 'vi' ? 'Mở đăng nhập' : 'Open sign in'}
              </button>
            </div>
          )}
        </label>

        <label>
          <span>Top-N</span>
          <input
            className="recommend-input"
            type="number"
            min={3}
            max={20}
            value={recoTopN}
            onChange={(event) => setRecoTopN(Number(event.target.value) || 10)}
          />
        </label>

        <label>
          <span>{locale === 'vi' ? 'Thể loại ưu tiên' : 'Preferred genre'}</span>
          <select className="recommend-input" value={recoGenre} onChange={(event) => setRecoGenre(event.target.value)}>
            <option value="">{locale === 'vi' ? 'Không lọc' : 'No filter'}</option>
            {quickGenres.map((genre) => (
              <option key={genre} value={genre}>
                {genre}
              </option>
            ))}
          </select>
        </label>
      </div>

      {isAuthenticated && (
        <div className="recommend-inline recommend-profile-summary">
          <span className="recommend-user-pill">
            {locale === 'vi' ? `Tài khoản: ${authUser?.ten_tai_khoan ?? ''}` : `Account: ${authUser?.ten_tai_khoan ?? ''}`}
          </span>
          <span className="recommend-user-pill">{profileModeLabel}</span>
        </div>
      )}

      <div className="recommend-genre-tags">
        {quickGenres.map((genre) => (
          <button
            key={genre}
            type="button"
            className={`recommend-chip-btn ${recoGenre === genre ? 'is-active' : ''}`}
            onClick={() => setRecoGenre(genre)}
          >
            {genre}
          </button>
        ))}
      </div>

      <div className="recommend-actions">
        <button
          type="button"
          className="primary-cta recommend-run-btn"
          onMouseDown={onBurst}
          onClick={onFetchRecommendations}
          disabled={recoLoading || !isAuthenticated}
        >
          {recoLoading
            ? locale === 'vi'
              ? 'Đang lấy gợi ý...'
              : 'Fetching...'
            : !isAuthenticated
              ? locale === 'vi'
                ? 'Đăng nhập để lấy gợi ý'
                : 'Sign in to get recommendations'
            : locale === 'vi'
              ? 'Lấy gợi ý phim'
              : 'Get movie recommendations'}
        </button>
        <button
          type="button"
          className="secondary-cta recommend-run-btn"
          onMouseDown={onBurst}
          onClick={onOpenFeedback}
          disabled={!isAuthenticated}
        >
          {locale === 'vi' ? `Phản hồi của bạn (${ratedCount})` : `Your feedback (${ratedCount})`}
        </button>
      </div>
    </div>
  )
}

type RecommendationCardProps = {
  locale: Locale
  row: RecommendationRow
  index: number
  total: number
  posterUrl: string
  isSaved: boolean
  ratingValue: number
  isRatingOpen: boolean
  scoreLabel: string
  year: string
  explainText: string
  onOpenDetail: () => void
  onToggleSave: () => void
  onLike: () => void
  onDislike: () => void
  onOpenRating: () => void
  onSetRating: (value: number) => void
  onSubmitRating: () => void
}

function RecommendationCard({
  locale,
  row,
  index,
  total,
  posterUrl,
  isSaved,
  ratingValue,
  isRatingOpen,
  scoreLabel,
  year,
  explainText,
  onOpenDetail,
  onToggleSave,
  onLike,
  onDislike,
  onOpenRating,
  onSetRating,
  onSubmitRating,
}: RecommendationCardProps) {
  const rankingLabel =
    index <= Math.ceil(total * 0.2)
      ? locale === 'vi'
        ? 'Đề xuất nổi bật'
        : 'Strong pick'
      : index <= Math.ceil(total * 0.55)
        ? locale === 'vi'
          ? 'Ưu tiên cho bạn'
          : 'High match'
        : locale === 'vi'
          ? 'Mức phù hợp cao'
          : 'Worth watching'

  return (
    <article className="recommend-row">
      <div className="recommend-visual">
        <img className="recommend-poster" src={posterUrl} alt={row.title} loading="lazy" />
        <span className="recommend-rank">#{index + 1}</span>
      </div>

      <div className="recommend-info">
        <div className="recommend-main-col">
          <div className="recommend-title-row">
            <p className="recommend-title">
              {row.title}
              {year && <span className="recommend-year">{year}</span>}
            </p>
          </div>

          <div className="recommend-badges">
            <span className="recommend-level-badge">{rankingLabel}</span>
            {row.genres
              .split('|')
              .slice(0, 3)
              .map((genre) => (
                <span key={`${row.movieId}-${genre}`} className="recommend-tag">
                  {genre}
                </span>
              ))}
          </div>

          <p className="recommend-explain-line">{explainText}</p>
        </div>

        <div className="recommend-side-col">
          <span className="recommend-score">{scoreLabel}</span>

          <div className="recommend-item-actions">
            <button type="button" className="recommend-chip-btn" onClick={onOpenDetail}>
              {locale === 'vi' ? 'Xem chi tiết' : 'View detail'}
            </button>
            <button type="button" className={`recommend-chip-btn ${isSaved ? 'is-active' : ''}`} onClick={onToggleSave}>
              {isSaved ? (locale === 'vi' ? 'Đã lưu xem sau' : 'Saved') : locale === 'vi' ? 'Lưu xem sau' : 'Save for later'}
            </button>
            <button type="button" className="recommend-chip-btn" onClick={onOpenRating}>
              {locale === 'vi' ? 'Đánh giá' : 'Rate'}
            </button>
          </div>

          <div className="recommend-feedback-actions">
            <button type="button" className="recommend-chip-btn recommend-chip-lite" onClick={onLike}>
              👍 {locale === 'vi' ? 'Thích' : 'Like'}
            </button>
            <button type="button" className="recommend-chip-btn recommend-chip-lite" onClick={onDislike}>
              👎 {locale === 'vi' ? 'Không quan tâm' : 'Not interested'}
            </button>
          </div>

          <AnimatePresence initial={false}>
            {isRatingOpen && (
              <motion.div
                className="recommend-rating-reveal"
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -8 }}
                transition={{ duration: 0.24, ease: EASE_SMOOTH }}
              >
                <div className="recommend-rating-row">
                  {[1, 2, 3, 4, 5].map((star) => (
                  <button
                    key={`${row.movieId}-star-${star}`}
                    type="button"
                    className={`recommend-star-btn ${ratingValue >= star ? 'is-active' : ''}`}
                    onClick={() => onSetRating(star)}
                  >
                    ★
                  </button>
                ))}
                <button type="button" className="recommend-chip-btn recommend-chip-primary" onClick={onSubmitRating}>
                  {locale === 'vi' ? 'Gửi đánh giá' : 'Submit rating'}
                </button>
              </div>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </div>
    </article>
  )
}

type FeedbackPanelProps = {
  locale: Locale
  ratedCount: number
  watchlistCount: number
  activeUserId: number
  ratedRows: RecommendationRow[]
  watchlistRows: RecommendationRow[]
  ratingByMovie: Record<number, number>
  onOpenMovie: (row: RecommendationRow) => void
  onBackToDiscover: () => void
  onRefresh: () => void
  onBurst: (event: ReactMouseEvent<HTMLElement>) => void
}

function FeedbackPanel({
  locale,
  ratedCount,
  watchlistCount,
  activeUserId,
  ratedRows,
  watchlistRows,
  ratingByMovie,
  onOpenMovie,
  onBackToDiscover,
  onRefresh,
  onBurst,
}: FeedbackPanelProps) {
  return (
    <div className="recommend-feedback-board">
      <div className="recommend-feedback-stats">
        <article>
          <p>{locale === 'vi' ? 'Phim đã chấm' : 'Rated movies'}</p>
          <strong>{ratedCount}</strong>
        </article>
        <article>
          <p>{locale === 'vi' ? 'Danh sách xem sau' : 'Watchlist'}</p>
          <strong>{watchlistCount}</strong>
        </article>
        <article>
          <p>{locale === 'vi' ? 'Hồ sơ đang dùng' : 'Active profile'}</p>
          <strong>{locale === 'vi' ? `Hồ sơ ${activeUserId}` : `User ${activeUserId}`}</strong>
        </article>
      </div>

      {ratedRows.length > 0 ? (
        <div className="recommend-mini-list">
          {ratedRows.map((row) => (
            <div key={`rated-${row.movieId}`} className="recommend-mini-row">
              <span>{row.title}</span>
              <strong>{ratingByMovie[row.movieId]}/5</strong>
            </div>
          ))}
        </div>
      ) : (
        <p className="recommend-empty">
          {locale === 'vi'
            ? 'Bạn chưa đánh giá phim nào. Hãy quay lại bước Gợi ý để chấm sao một vài phim.'
            : 'No ratings yet. Return to Recommendations and rate a few movies.'}
        </p>
      )}

      {watchlistRows.length > 0 && (
        <div className="recommend-mini-list">
          {watchlistRows.map((row) => (
            <div key={`watch-${row.movieId}`} className="recommend-mini-row">
              <span>{row.title}</span>
              <button type="button" className="recommend-chip-btn" onClick={() => onOpenMovie(row)}>
                {locale === 'vi' ? 'Mở phim' : 'Open movie'}
              </button>
            </div>
          ))}
        </div>
      )}

      <div className="recommend-actions">
        <button type="button" className="primary-cta recommend-run-btn" onMouseDown={onBurst} onClick={onRefresh}>
          {locale === 'vi' ? 'Cập nhật lại gợi ý' : 'Refresh recommendations'}
        </button>
        <button type="button" className="secondary-cta recommend-run-btn" onMouseDown={onBurst} onClick={onBackToDiscover}>
          {locale === 'vi' ? 'Quay lại danh sách phim' : 'Back to movie list'}
        </button>
      </div>
    </div>
  )
}

type RecommendationAuthPanelProps = {
  locale: Locale
  isAuthenticated: boolean
  authView: 'login' | 'register'
  setAuthView: (view: 'login' | 'register') => void
  authUser: AuthUser | null
  profileCount: number
  authLoading: boolean
  authError: string
  loginIdentifier: string
  setLoginIdentifier: (value: string) => void
  loginPassword: string
  setLoginPassword: (value: string) => void
  registerFullName: string
  setRegisterFullName: (value: string) => void
  registerUsername: string
  setRegisterUsername: (value: string) => void
  registerEmail: string
  setRegisterEmail: (value: string) => void
  registerPassword: string
  setRegisterPassword: (value: string) => void
  registerConfirmPassword: string
  setRegisterConfirmPassword: (value: string) => void
  onLogin: () => void
  onRegister: () => void
  onDemoLogin: () => void
  onLogout: () => void
  onBurst: (event: ReactMouseEvent<HTMLElement>) => void
}

function RecommendationAuthPanel({
  locale,
  isAuthenticated,
  authView,
  setAuthView,
  authUser,
  profileCount,
  authLoading,
  authError,
  loginIdentifier,
  setLoginIdentifier,
  loginPassword,
  setLoginPassword,
  registerFullName,
  setRegisterFullName,
  registerUsername,
  setRegisterUsername,
  registerEmail,
  setRegisterEmail,
  registerPassword,
  setRegisterPassword,
  registerConfirmPassword,
  setRegisterConfirmPassword,
  onLogin,
  onRegister,
  onDemoLogin,
  onLogout,
  onBurst,
}: RecommendationAuthPanelProps) {
  if (isAuthenticated && authUser) {
    return (
      <div className="recommend-auth-panel is-signed-in">
        <div>
          <p className="recommend-auth-kicker">{locale === 'vi' ? 'Tài khoản hiện tại' : 'Current account'}</p>
          <h3>{authUser.ho_ten || authUser.ten_tai_khoan}</h3>
          <p className="recommend-auth-meta">
            {locale === 'vi'
              ? `@${authUser.ten_tai_khoan} • ${profileCount} hồ sơ`
              : `@${authUser.ten_tai_khoan} • ${profileCount} profiles`}
          </p>
        </div>
        <button type="button" className="secondary-cta recommend-run-btn" onMouseDown={onBurst} onClick={onLogout}>
          {locale === 'vi' ? 'Đăng xuất' : 'Sign out'}
        </button>
      </div>
    )
  }

  return (
    <div className="recommend-auth-panel">
      <div className="recommend-auth-head">
        <div className="recommend-auth-tabs">
          <button
            type="button"
            className={`recommend-chip-btn ${authView === 'login' ? 'is-active' : ''}`}
            onClick={() => setAuthView('login')}
          >
            {locale === 'vi' ? 'Đăng nhập' : 'Sign in'}
          </button>
          <button
            type="button"
            className={`recommend-chip-btn ${authView === 'register' ? 'is-active' : ''}`}
            onClick={() => setAuthView('register')}
          >
            {locale === 'vi' ? 'Đăng ký' : 'Sign up'}
          </button>
        </div>
        <button type="button" className="recommend-chip-btn recommend-chip-lite" onClick={onDemoLogin} onMouseDown={onBurst}>
          {locale === 'vi' ? 'Dùng thử nhanh (demo)' : 'Quick demo login'}
        </button>
      </div>

      {authView === 'login' ? (
        <div className="recommend-auth-form">
          <label>
            <span>{locale === 'vi' ? 'Tên tài khoản hoặc email' : 'Username or email'}</span>
            <input
              className="recommend-input"
              value={loginIdentifier}
              onChange={(event) => setLoginIdentifier(event.target.value)}
              placeholder={locale === 'vi' ? 'demo hoặc demo@khangdauti.local' : 'demo or demo@khangdauti.local'}
            />
          </label>
          <label>
            <span>{locale === 'vi' ? 'Mật khẩu' : 'Password'}</span>
            <input
              className="recommend-input"
              type="password"
              value={loginPassword}
              onChange={(event) => setLoginPassword(event.target.value)}
              placeholder="••••••••"
            />
          </label>
          <button
            type="button"
            className="primary-cta recommend-run-btn"
            onMouseDown={onBurst}
            onClick={onLogin}
            disabled={authLoading}
          >
            {authLoading
              ? locale === 'vi'
                ? 'Đang đăng nhập...'
                : 'Signing in...'
              : locale === 'vi'
                ? 'Đăng nhập và mở hồ sơ'
                : 'Sign in and load profiles'}
          </button>
        </div>
      ) : (
        <div className="recommend-auth-form">
          <label>
            <span>{locale === 'vi' ? 'Họ và tên' : 'Full name'}</span>
            <input
              className="recommend-input"
              value={registerFullName}
              onChange={(event) => setRegisterFullName(event.target.value)}
            />
          </label>
          <label>
            <span>{locale === 'vi' ? 'Tên tài khoản' : 'Username'}</span>
            <input
              className="recommend-input"
              value={registerUsername}
              onChange={(event) => setRegisterUsername(event.target.value)}
            />
          </label>
          <label>
            <span>Email</span>
            <input className="recommend-input" value={registerEmail} onChange={(event) => setRegisterEmail(event.target.value)} />
          </label>
          <label>
            <span>{locale === 'vi' ? 'Mật khẩu' : 'Password'}</span>
            <input
              className="recommend-input"
              type="password"
              value={registerPassword}
              onChange={(event) => setRegisterPassword(event.target.value)}
            />
          </label>
          <label>
            <span>{locale === 'vi' ? 'Xác nhận mật khẩu' : 'Confirm password'}</span>
            <input
              className="recommend-input"
              type="password"
              value={registerConfirmPassword}
              onChange={(event) => setRegisterConfirmPassword(event.target.value)}
            />
          </label>
          <button
            type="button"
            className="primary-cta recommend-run-btn"
            onMouseDown={onBurst}
            onClick={onRegister}
            disabled={authLoading}
          >
            {authLoading ? (locale === 'vi' ? 'Đang tạo tài khoản...' : 'Creating account...') : locale === 'vi' ? 'Tạo tài khoản' : 'Create account'}
          </button>
        </div>
      )}

      {authError && <p className="recommend-error">{authError}</p>}
    </div>
  )
}

function RecommendationHero({ locale }: { locale: Locale }) {
  return (
    <section className="recommend-hero-section">
      <div className="recommend-hero-backdrop" aria-hidden />
      <div className="recommend-hero-overlay" aria-hidden />
      <div className="recommend-content-rail">
        <div className="recommend-hero-content">
          <p className="section-kicker">
            {locale === 'vi' ? 'Trung tâm gợi ý cá nhân hóa' : 'Personalized recommendation center'}
          </p>
          <h1>{locale === 'vi' ? 'GỢI Ý PHIM' : 'MOVIE RECOMMENDATIONS'}</h1>
          <p>
            {locale === 'vi'
              ? 'Khởi tạo hồ sơ, nhận gợi ý phù hợp và phản hồi để hệ thống học gu xem của bạn theo thời gian.'
              : 'Initialize profile, get relevant recommendations, and send feedback so the system learns your taste over time.'}
          </p>
        </div>
      </div>
    </section>
  )
}

function RecommendationStepper(props: RecommendationHeaderProps) {
  return <RecommendationHeader {...props} />
}

function UserProfileModeCard(props: UserModeSelectorProps) {
  return <UserModeSelector {...props} />
}

type RecommendationControlsProps = {
  locale: Locale
  isAuthenticated: boolean
  authView: 'login' | 'register'
  setAuthView: (view: 'login' | 'register') => void
  authUser: AuthUser | null
  authProfiles: UserProfile[]
  activeProfileId: number | null
  setActiveProfileId: (id: number) => void
  authLoading: boolean
  authError: string
  loginIdentifier: string
  setLoginIdentifier: (value: string) => void
  loginPassword: string
  setLoginPassword: (value: string) => void
  registerFullName: string
  setRegisterFullName: (value: string) => void
  registerUsername: string
  setRegisterUsername: (value: string) => void
  registerEmail: string
  setRegisterEmail: (value: string) => void
  registerPassword: string
  setRegisterPassword: (value: string) => void
  registerConfirmPassword: string
  setRegisterConfirmPassword: (value: string) => void
  onLogin: () => void
  onRegister: () => void
  onDemoLogin: () => void
  onLogout: () => void
  stageTabs: ReadonlyArray<{ key: RecoStage; label: string }>
  recoStage: RecoStage
  setRecoStage: (stage: RecoStage) => void
  apiHealth: string
  routeBadge: string
  routeExplain: string
  userMode: UserMode
  setUserMode: (mode: UserMode) => void
  recoTopN: number
  setRecoTopN: (value: number) => void
  recoGenre: string
  setRecoGenre: (genre: string) => void
  quickGenres: string[]
  recoLoading: boolean
  ratedCount: number
  onOpenFeedback: () => void
  onFetchRecommendations: () => void
  onBurst: (event: ReactMouseEvent<HTMLElement>) => void
  showOnboardingHint: boolean
}

function RecommendationControls({
  locale,
  isAuthenticated,
  authView,
  setAuthView,
  authUser,
  authProfiles,
  activeProfileId,
  setActiveProfileId,
  authLoading,
  authError,
  loginIdentifier,
  setLoginIdentifier,
  loginPassword,
  setLoginPassword,
  registerFullName,
  setRegisterFullName,
  registerUsername,
  setRegisterUsername,
  registerEmail,
  setRegisterEmail,
  registerPassword,
  setRegisterPassword,
  registerConfirmPassword,
  setRegisterConfirmPassword,
  onLogin,
  onRegister,
  onDemoLogin,
  onLogout,
  stageTabs,
  recoStage,
  setRecoStage,
  apiHealth,
  routeBadge,
  routeExplain,
  userMode,
  setUserMode,
  recoTopN,
  setRecoTopN,
  recoGenre,
  setRecoGenre,
  quickGenres,
  recoLoading,
  ratedCount,
  onOpenFeedback,
  onFetchRecommendations,
  onBurst,
  showOnboardingHint,
}: RecommendationControlsProps) {
  const activeProfile = authProfiles.find((profile) => profile.id_ho_so === activeProfileId) ?? null

  return (
    <section className="recommend-controls-section">
      <div className="recommend-content-rail">
        <RecommendationAuthPanel
          locale={locale}
          isAuthenticated={isAuthenticated}
          authView={authView}
          setAuthView={setAuthView}
          authUser={authUser}
          profileCount={authProfiles.length}
          authLoading={authLoading}
          authError={authError}
          loginIdentifier={loginIdentifier}
          setLoginIdentifier={setLoginIdentifier}
          loginPassword={loginPassword}
          setLoginPassword={setLoginPassword}
          registerFullName={registerFullName}
          setRegisterFullName={setRegisterFullName}
          registerUsername={registerUsername}
          setRegisterUsername={setRegisterUsername}
          registerEmail={registerEmail}
          setRegisterEmail={setRegisterEmail}
          registerPassword={registerPassword}
          setRegisterPassword={setRegisterPassword}
          registerConfirmPassword={registerConfirmPassword}
          setRegisterConfirmPassword={setRegisterConfirmPassword}
          onLogin={onLogin}
          onRegister={onRegister}
          onDemoLogin={onDemoLogin}
          onLogout={onLogout}
          onBurst={onBurst}
        />

        {isAuthenticated ? (
          <>
        <RecommendationStepper
          locale={locale}
          stageTabs={stageTabs}
          recoStage={recoStage}
          setRecoStage={setRecoStage}
          apiHealth={apiHealth}
          routeBadge={routeBadge}
          routeExplain={routeExplain}
        />

        <div className="recommend-top-grid">
          <div className="recommend-flow-col">
            <UserProfileModeCard
              locale={locale}
              userMode={userMode}
              onSelectMode={setUserMode}
            />

            {showOnboardingHint && (
              <div className="recommend-stage-card">
                <h3>{locale === 'vi' ? 'Bắt đầu hành trình gợi ý' : 'Start recommendation journey'}</h3>
                <p>
                  {locale === 'vi'
                    ? 'Chọn loại hồ sơ, đặt Top-N và thể loại ưu tiên, sau đó bấm "Lấy gợi ý phim". Sau mỗi lần phản hồi, chất lượng gợi ý sẽ được cá nhân hóa tốt hơn.'
                    : 'Choose profile type, set Top-N and preferred genre, then click "Get movie recommendations". Feedback from each session improves personalization.'}
                </p>
              </div>
            )}
          </div>

          <FilterBar
            locale={locale}
            isAuthenticated={isAuthenticated}
            authUser={authUser}
            profiles={authProfiles}
            activeProfileId={activeProfileId}
            setActiveProfileId={setActiveProfileId}
            recoTopN={recoTopN}
            setRecoTopN={setRecoTopN}
            recoGenre={recoGenre}
            setRecoGenre={setRecoGenre}
            quickGenres={quickGenres}
            recoLoading={recoLoading}
            ratedCount={ratedCount}
            onBurst={onBurst}
            onOpenFeedback={onOpenFeedback}
            onFetchRecommendations={onFetchRecommendations}
            onOpenAuth={() => setAuthView('login')}
          />
        </div>
          </>
        ) : (
          <p className="recommend-login-gate">
            {locale === 'vi'
              ? 'Đăng nhập hoặc dùng thử demo để mở hồ sơ và bắt đầu luồng gợi ý phim.'
              : 'Sign in or use demo login to open profiles and start the recommendation flow.'}
          </p>
        )}

        {isAuthenticated && !activeProfile && (
          <p className="recommend-login-gate">
            {locale === 'vi'
              ? 'Tài khoản này chưa có hồ sơ. Vui lòng tạo hồ sơ để tiếp tục.'
              : 'This account has no profile yet. Create one to continue.'}
          </p>
        )}
      </div>
    </section>
  )
}

function RecommendationMovieCard(props: RecommendationCardProps) {
  return <RecommendationCard {...props} />
}

type RecommendationResultsSectionProps = {
  locale: Locale
  rows: RecommendationRow[]
  recoTopN: number
  userMode: UserMode
  posterByMovie: Record<number, string>
  watchlist: number[]
  ratingByMovie: Record<number, number>
  ratingPanelMovieId: number | null
  scoreLabel: (row: RecommendationRow, index: number, total: number) => string
  extractMovieYear: (title: string) => string
  setDetailMovie: (row: RecommendationRow) => void
  postEvent: (eventType: 'click' | 'view' | 'skip' | 'rate', movieId: number, metadata?: Record<string, unknown>) => Promise<void>
  toggleWatchlist: (movieId: number) => void
  setFeedbackNote: (text: string) => void
  setRatingPanelMovieId: (movieId: number | null | ((prev: number | null) => number | null)) => void
  setRatingByMovie: (updater: (prev: Record<number, number>) => Record<number, number>) => void
  submitRating: (movieId: number, rating: number) => Promise<void>
}

function RecommendationResultsSection({
  locale,
  rows,
  recoTopN,
  userMode,
  posterByMovie,
  watchlist,
  ratingByMovie,
  ratingPanelMovieId,
  scoreLabel,
  extractMovieYear,
  setDetailMovie,
  postEvent,
  toggleWatchlist,
  setFeedbackNote,
  setRatingPanelMovieId,
  setRatingByMovie,
  submitRating,
}: RecommendationResultsSectionProps) {
  return (
    <section className="recommend-results-section">
      <div className="recommend-content-rail">
        <div className="recommend-results-head">
          <h2>{locale === 'vi' ? 'Danh sách phim đề xuất cho bạn' : 'Recommended titles for you'}</h2>
          <p>
            {locale === 'vi'
              ? 'Sắp xếp theo mức độ phù hợp từ hành vi gần đây và sở thích hồ sơ hiện tại.'
              : 'Ranked by relevance and most recent behavior signals.'}
          </p>
        </div>

        <div className="recommend-list">
          {rows.slice(0, recoTopN).map((row, index) => {
            const year = extractMovieYear(row.title)
            const explainText =
              row.explain ||
              (userMode === 'existing'
                ? locale === 'vi'
                  ? 'Đề xuất dựa trên lịch sử đánh giá trước đó và gu xem phim của bạn.'
                  : 'Suggested from your historical ratings and taste profile.'
                : locale === 'vi'
                  ? 'Đề xuất theo thể loại ưu tiên và độ phổ biến của nhóm phim tương tự.'
                  : 'Suggested by preferred genre and popularity among similar titles.')

            return (
              <RecommendationMovieCard
                key={`${row.movieId}-${index}`}
                locale={locale}
                row={row}
                index={index}
                total={rows.length || 1}
                posterUrl={posterByMovie[row.movieId] ?? `https://picsum.photos/seed/movie-${row.movieId}/160/220`}
                isSaved={watchlist.includes(row.movieId)}
                ratingValue={ratingByMovie[row.movieId] ?? 0}
                isRatingOpen={ratingPanelMovieId === row.movieId}
                scoreLabel={scoreLabel(row, index, rows.length || 1)}
                year={year}
                explainText={explainText}
                onOpenDetail={() => {
                  setDetailMovie(row)
                  void postEvent('view', row.movieId, { source: 'recommend-list' })
                }}
                onToggleSave={() => {
                  const saved = watchlist.includes(row.movieId)
                  toggleWatchlist(row.movieId)
                  void postEvent('click', row.movieId, { action: saved ? 'unsave_later' : 'save_later' })
                  setFeedbackNote(
                    locale === 'vi'
                      ? saved
                        ? 'Đã bỏ khỏi danh sách xem sau.'
                        : 'Đã lưu vào danh sách xem sau.'
                      : saved
                        ? 'Removed from watchlist.'
                        : 'Saved to watchlist.',
                  )
                }}
                onLike={() => {
                  void postEvent('click', row.movieId, { action: 'like' })
                  setFeedbackNote(locale === 'vi' ? 'Đã ghi nhận: bạn thích phim này.' : 'Preference saved: you liked this movie.')
                }}
                onDislike={() => {
                  void postEvent('skip', row.movieId, { action: 'dislike' })
                  setFeedbackNote(
                    locale === 'vi' ? 'Đã ghi nhận: bạn không quan tâm phim này.' : 'Preference saved: not interested.',
                  )
                }}
                onOpenRating={() => {
                  setRatingPanelMovieId((prev) => (prev === row.movieId ? null : row.movieId))
                }}
                onSetRating={(value) => {
                  setRatingByMovie((prev) => ({ ...prev, [row.movieId]: value }))
                }}
                onSubmitRating={() => {
                  const rating = ratingByMovie[row.movieId] ?? 4
                  void submitRating(row.movieId, rating)
                  setRatingPanelMovieId(null)
                }}
              />
            )
          })}

          {!rows.length && (
            <p className="recommend-empty">
              {locale === 'vi'
                ? 'Nhấn "Lấy gợi ý phim" để hiển thị danh sách gợi ý tại đây.'
                : 'Click "Get movie recommendations" to load personalized results here.'}
            </p>
          )}
        </div>
      </div>
    </section>
  )
}

function RecommendationFeedbackDrawer(props: FeedbackPanelProps) {
  return (
    <section className="recommend-feedback-section">
      <div className="recommend-content-rail">
        <FeedbackPanel {...props} />
      </div>
    </section>
  )
}

type RecommendationPageShellProps = {
  locale: Locale
  isAuthenticated: boolean
  authView: 'login' | 'register'
  setAuthView: (view: 'login' | 'register') => void
  authUser: AuthUser | null
  authProfiles: UserProfile[]
  activeProfileId: number | null
  setActiveProfileId: (id: number) => void
  activeRecoUserId: number
  authLoading: boolean
  authError: string
  loginIdentifier: string
  setLoginIdentifier: (value: string) => void
  loginPassword: string
  setLoginPassword: (value: string) => void
  registerFullName: string
  setRegisterFullName: (value: string) => void
  registerUsername: string
  setRegisterUsername: (value: string) => void
  registerEmail: string
  setRegisterEmail: (value: string) => void
  registerPassword: string
  setRegisterPassword: (value: string) => void
  registerConfirmPassword: string
  setRegisterConfirmPassword: (value: string) => void
  onLogin: () => void
  onRegister: () => void
  onDemoLogin: () => void
  onLogout: () => void
  featuredRef: (node: HTMLElement | null) => void
  stageTabs: ReadonlyArray<{ key: RecoStage; label: string }>
  recoStage: RecoStage
  setRecoStage: (stage: RecoStage) => void
  apiHealth: string
  routeBadge: string
  routeExplain: string
  userMode: UserMode
  setUserMode: (mode: UserMode) => void
  recoTopN: number
  setRecoTopN: (value: number) => void
  recoGenre: string
  setRecoGenre: (genre: string) => void
  quickGenres: string[]
  recoLoading: boolean
  ratedMovieIds: number[]
  recommendationRows: RecommendationRow[]
  posterByMovie: Record<number, string>
  watchlist: number[]
  watchlistRows: RecommendationRow[]
  ratingByMovie: Record<number, number>
  ratedRows: RecommendationRow[]
  ratingPanelMovieId: number | null
  scoreLabel: (row: RecommendationRow, index: number, total: number) => string
  extractMovieYear: (title: string) => string
  setDetailMovie: (row: RecommendationRow) => void
  postEvent: (eventType: 'click' | 'view' | 'skip' | 'rate', movieId: number, metadata?: Record<string, unknown>) => Promise<void>
  toggleWatchlist: (movieId: number) => void
  setFeedbackNote: (text: string) => void
  setRatingPanelMovieId: (movieId: number | null | ((prev: number | null) => number | null)) => void
  setRatingByMovie: (updater: (prev: Record<number, number>) => Record<number, number>) => void
  submitRating: (movieId: number, rating: number) => Promise<void>
  fetchRecommendations: () => Promise<void>
  handleBurst: (event: ReactMouseEvent<HTMLElement>) => void
  recoError: string
  feedbackNote: string
}

function RecommendationPageShell({
  locale,
  isAuthenticated,
  authView,
  setAuthView,
  authUser,
  authProfiles,
  activeProfileId,
  setActiveProfileId,
  activeRecoUserId,
  authLoading,
  authError,
  loginIdentifier,
  setLoginIdentifier,
  loginPassword,
  setLoginPassword,
  registerFullName,
  setRegisterFullName,
  registerUsername,
  setRegisterUsername,
  registerEmail,
  setRegisterEmail,
  registerPassword,
  setRegisterPassword,
  registerConfirmPassword,
  setRegisterConfirmPassword,
  onLogin,
  onRegister,
  onDemoLogin,
  onLogout,
  featuredRef,
  stageTabs,
  recoStage,
  setRecoStage,
  apiHealth,
  routeBadge,
  routeExplain,
  userMode,
  setUserMode,
  recoTopN,
  setRecoTopN,
  recoGenre,
  setRecoGenre,
  quickGenres,
  recoLoading,
  ratedMovieIds,
  recommendationRows,
  posterByMovie,
  watchlist,
  watchlistRows,
  ratingByMovie,
  ratedRows,
  ratingPanelMovieId,
  scoreLabel,
  extractMovieYear,
  setDetailMovie,
  postEvent,
  toggleWatchlist,
  setFeedbackNote,
  setRatingPanelMovieId,
  setRatingByMovie,
  submitRating,
  fetchRecommendations,
  handleBurst,
  recoError,
  feedbackNote,
}: RecommendationPageShellProps) {
  return (
    <div className="recommend-page-shell">
      <motion.section
        ref={featuredRef}
        data-scene="featured"
        className="scene-section scroll-mt-24"
        initial={{ opacity: 0, y: 22 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ amount: 0.3, once: true }}
        transition={{ duration: 0.62, ease: EASE_SMOOTH }}
      >
        <RecommendationHero locale={locale} />
      </motion.section>

      <RecommendationControls
        locale={locale}
        isAuthenticated={isAuthenticated}
        authView={authView}
        setAuthView={setAuthView}
        authUser={authUser}
        authProfiles={authProfiles}
        activeProfileId={activeProfileId}
        setActiveProfileId={(profileId) => {
          setActiveProfileId(profileId)
          setRecoStage('onboard')
          setRatingPanelMovieId(null)
        }}
        authLoading={authLoading}
        authError={authError}
        loginIdentifier={loginIdentifier}
        setLoginIdentifier={setLoginIdentifier}
        loginPassword={loginPassword}
        setLoginPassword={setLoginPassword}
        registerFullName={registerFullName}
        setRegisterFullName={setRegisterFullName}
        registerUsername={registerUsername}
        setRegisterUsername={setRegisterUsername}
        registerEmail={registerEmail}
        setRegisterEmail={setRegisterEmail}
        registerPassword={registerPassword}
        setRegisterPassword={setRegisterPassword}
        registerConfirmPassword={registerConfirmPassword}
        setRegisterConfirmPassword={setRegisterConfirmPassword}
        onLogin={onLogin}
        onRegister={onRegister}
        onDemoLogin={onDemoLogin}
        onLogout={onLogout}
        stageTabs={stageTabs}
        recoStage={recoStage}
        setRecoStage={setRecoStage}
        apiHealth={apiHealth}
        routeBadge={routeBadge}
        routeExplain={routeExplain}
        userMode={userMode}
        setUserMode={(mode) => {
          setUserMode(mode)
          const matchedProfile = authProfiles.find((profile) =>
            mode === 'existing'
              ? profile.che_do_goi_y === 'ca_nhan_hoa'
              : profile.che_do_goi_y !== 'ca_nhan_hoa',
          )
          if (matchedProfile) {
            setActiveProfileId(matchedProfile.id_ho_so)
          }
          setRecoStage('onboard')
          setRatingPanelMovieId(null)
        }}
        recoTopN={recoTopN}
        setRecoTopN={setRecoTopN}
        recoGenre={recoGenre}
        setRecoGenre={setRecoGenre}
        quickGenres={quickGenres}
        recoLoading={recoLoading}
        ratedCount={ratedMovieIds.length}
        onBurst={handleBurst}
        onOpenFeedback={() => {
          setRecoStage('feedback')
          setRatingPanelMovieId(null)
        }}
        onFetchRecommendations={() => {
          setRatingPanelMovieId(null)
          void fetchRecommendations()
        }}
        showOnboardingHint={recoStage === 'onboard'}
      />

      {isAuthenticated && (
        <div className="recommend-content-rail recommend-status-notes">
          {recoError && <p className="recommend-error">{recoError}</p>}
          {feedbackNote && <p className="recommend-feedback-note">{feedbackNote}</p>}
          {watchlist.length > 0 && (
            <p className="recommend-watchlist-note">
              {locale === 'vi'
                ? `Đã lưu xem sau: ${watchlist.length} phim`
                : `Watchlist saved: ${watchlist.length} movies`}
            </p>
          )}
        </div>
      )}

      {isAuthenticated && recoStage === 'discover' && (
        <RecommendationResultsSection
          locale={locale}
          rows={recommendationRows}
          recoTopN={recoTopN}
          userMode={userMode}
          posterByMovie={posterByMovie}
          watchlist={watchlist}
          ratingByMovie={ratingByMovie}
          ratingPanelMovieId={ratingPanelMovieId}
          scoreLabel={scoreLabel}
          extractMovieYear={extractMovieYear}
          setDetailMovie={setDetailMovie}
          postEvent={postEvent}
          toggleWatchlist={toggleWatchlist}
          setFeedbackNote={setFeedbackNote}
          setRatingPanelMovieId={setRatingPanelMovieId}
          setRatingByMovie={setRatingByMovie}
          submitRating={submitRating}
        />
      )}

      {isAuthenticated && recoStage === 'feedback' && (
        <RecommendationFeedbackDrawer
          locale={locale}
          ratedCount={ratedMovieIds.length}
          watchlistCount={watchlist.length}
          activeUserId={activeRecoUserId}
          ratedRows={ratedRows}
          watchlistRows={watchlistRows}
          ratingByMovie={ratingByMovie}
          onOpenMovie={(row) => setDetailMovie(row)}
          onBurst={handleBurst}
          onBackToDiscover={() => {
            setRecoStage('discover')
            setRatingPanelMovieId(null)
          }}
          onRefresh={() => {
            setRecoStage('discover')
            setRatingPanelMovieId(null)
            void fetchRecommendations()
          }}
        />
      )}
    </div>
  )
}

function App() {
  const reduceMotion = useReducedMotion()
  const [locale, setLocale] = useState<Locale>('vi')
  const [authView, setAuthView] = useState<'login' | 'register'>('login')
  const [authToken, setAuthToken] = useState<string>('')
  const [authUser, setAuthUser] = useState<AuthUser | null>(null)
  const [authProfiles, setAuthProfiles] = useState<UserProfile[]>([])
  const [activeProfileId, setActiveProfileId] = useState<number | null>(null)
  const [authLoading, setAuthLoading] = useState<boolean>(false)
  const [authError, setAuthError] = useState<string>('')
  const [loginIdentifier, setLoginIdentifier] = useState<string>('demo')
  const [loginPassword, setLoginPassword] = useState<string>('demo123')
  const [registerFullName, setRegisterFullName] = useState<string>('')
  const [registerUsername, setRegisterUsername] = useState<string>('')
  const [registerEmail, setRegisterEmail] = useState<string>('')
  const [registerPassword, setRegisterPassword] = useState<string>('')
  const [registerConfirmPassword, setRegisterConfirmPassword] = useState<string>('')
  const [introStep, setIntroStep] = useState<number>(0)
  const [activeSection, setActiveSection] = useState<SectionId>('home')
  const [focusedService, setFocusedService] = useState<number>(1)
  const [focusedWork, setFocusedWork] = useState<number>(0)
  const [localeFlash, setLocaleFlash] = useState<boolean>(false)
  const [bursts, setBursts] = useState<Burst[]>([])
  const [cursorVisible, setCursorVisible] = useState(false)
  const [cursorLabel, setCursorLabel] = useState<string>('')
  const [sceneFlash, setSceneFlash] = useState<boolean>(false)
  const [pageMode, setPageMode] = useState<PageMode>('landing')
  const [userMode, setUserMode] = useState<UserMode>('existing')
  const [recoStage, setRecoStage] = useState<RecoStage>('onboard')
  const [recoTopN, setRecoTopN] = useState<number>(10)
  const [recoGenre, setRecoGenre] = useState<string>('Action')
  const [recoLoading, setRecoLoading] = useState<boolean>(false)
  const [recoError, setRecoError] = useState<string>('')
  const [feedbackNote, setFeedbackNote] = useState<string>('')
  const [recoPayload, setRecoPayload] = useState<RecommendationPayload | null>(null)
  const [apiHealth, setApiHealth] = useState<string>('checking')
  const [watchlist, setWatchlist] = useState<number[]>([])
  const [ratingByMovie, setRatingByMovie] = useState<Record<number, number>>({})
  const [ratingPanelMovieId, setRatingPanelMovieId] = useState<number | null>(null)
  const [posterByMovie, setPosterByMovie] = useState<Record<number, string>>({})
  const [detailMovie, setDetailMovie] = useState<RecommendationRow | null>(null)
  const localeTimerRef = useRef<number | null>(null)

  const sectionsRef = useRef<Record<SectionId, HTMLElement | null>>({
    home: null,
    services: null,
    featured: null,
    works: null,
    awards: null,
    about: null,
    clients: null,
    contact: null,
  })

  const burstIdRef = useRef<number>(0)
  const heroRef = useRef<HTMLElement | null>(null)

  const [mouse, setMouse] = useState({ x: -200, y: -200 })
  const pointerFine = useMemo(() => {
    if (typeof window === 'undefined') {
      return false
    }
    return window.matchMedia('(pointer: fine)').matches
  }, [])

  const { scrollYProgress: heroScrollProgress } = useScroll({
    target: heroRef,
    offset: ['start start', 'end start'],
  })
  const heroTitleY = useTransform(heroScrollProgress, [0, 1], [0, 120])
  const heroPanelY = useTransform(heroScrollProgress, [0, 1], [0, 65])
  const heroOpacity = useTransform(heroScrollProgress, [0, 0.8, 1], [1, 0.92, 0.68])
  const stageStep = reduceMotion ? 3 : introStep
  const copy = locale === 'vi' ? TEXT.vi : TEXT.en
  const navItems = locale === 'vi' ? NAV_ITEMS_VI : NAV_ITEMS_EN
  const stats = locale === 'vi' ? STATS_VI : STATS_EN
  const services = locale === 'vi' ? SERVICES_VI : SERVICES_EN
  const works = locale === 'vi' ? WORKS_VI : WORKS_EN
  const awards = locale === 'vi' ? AWARDS_VI : AWARDS_EN
  const activeProfile = useMemo(
    () => authProfiles.find((profile) => profile.id_ho_so === activeProfileId) ?? null,
    [activeProfileId, authProfiles],
  )
  const isAuthenticated = Boolean(authToken && authUser)
  const activeRecoUserId = activeProfile?.id_user_ml ?? (userMode === 'existing' ? 2 : 999999)
  const quickGenres = ['Action', 'Drama', 'Comedy', 'Sci-Fi', 'Thriller', 'Romance']
  const stageTabs = useMemo(
    () =>
      locale === 'vi'
        ? [
            { key: 'onboard' as const, label: 'Khởi tạo' },
            { key: 'discover' as const, label: 'Gợi ý' },
            { key: 'feedback' as const, label: 'Phản hồi' },
          ]
        : [
            { key: 'onboard' as const, label: 'Onboarding' },
            { key: 'discover' as const, label: 'Recommendations' },
            { key: 'feedback' as const, label: 'Feedback' },
          ],
    [locale],
  )
  const apiBase = useMemo(() => {
    const envBase = (import.meta.env.VITE_API_BASE_URL as string | undefined)?.trim()
    if (envBase) {
      return envBase.replace(/\/$/, '')
    }
    if (typeof window !== 'undefined') {
      const currentOrigin = window.location.origin.replace(/\/$/, '')
      if (window.location.port === '8000') {
        return currentOrigin
      }
      return 'http://127.0.0.1:8000'
    }
    return ''
  }, [])

  const buildApiUrl = useCallback(
    (path: string) => `${apiBase}${path.startsWith('/') ? path : `/${path}`}`,
    [apiBase],
  )

  const formatApiError = useCallback(
    (error: unknown): string => {
      const raw = error instanceof Error ? error.message : String(error)
      if (/Failed to fetch|NetworkError|ERR_CONNECTION_REFUSED/i.test(raw)) {
        return locale === 'vi'
          ? 'Không kết nối được API (127.0.0.1:8000). Hãy chạy lại backend rồi thử lại.'
          : 'Could not connect to API (127.0.0.1:8000). Please restart backend and retry.'
      }
      return raw
    },
    [locale],
  )

  const clearSession = useCallback(() => {
    setAuthToken('')
    setAuthUser(null)
    setAuthProfiles([])
    setActiveProfileId(null)
    setRecoPayload(null)
    setRecoStage('onboard')
    if (typeof window !== 'undefined') {
      window.localStorage.removeItem('mr_token')
      window.localStorage.removeItem('mr_profile_id')
      window.localStorage.removeItem('khangdauti_token')
      window.localStorage.removeItem('khangdauti_user')
    }
  }, [])

  const loadProfiles = useCallback(
    async (token: string) => {
      const response = await fetch(buildApiUrl('/ho-so'), {
        headers: { Authorization: `Bearer ${token}` },
      })
      if (!response.ok) {
        throw new Error(await response.text())
      }
      const payload = (await response.json()) as { ho_so?: UserProfile[] }
      const profiles = payload.ho_so ?? []
      const savedProfileId =
        typeof window !== 'undefined' ? Number(window.localStorage.getItem('mr_profile_id') ?? '') : Number.NaN
      const preferredProfileId =
        Number.isFinite(savedProfileId) && profiles.some((profile) => profile.id_ho_so === savedProfileId)
          ? savedProfileId
          : profiles[0]?.id_ho_so ?? null
      setAuthProfiles(profiles)
      setActiveProfileId((prev) => prev ?? preferredProfileId)
      return profiles
    },
    [buildApiUrl],
  )

  const loginWithCredentials = useCallback(
    async (identifier: string, password: string) => {
      const response = await fetch(buildApiUrl('/dang-nhap'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenDangNhap: identifier.trim(),
          matKhau: password,
        }),
      })
      if (!response.ok) {
        throw new Error((await response.text()) || `${response.status}`)
      }
      const payload = (await response.json()) as {
        token: string
        nguoi_dung: AuthUser
        ho_so?: UserProfile[]
      }
      setAuthToken(payload.token)
      setAuthUser(payload.nguoi_dung)
      const profiles = payload.ho_so ?? []
      setAuthProfiles(profiles)
      setActiveProfileId(profiles[0]?.id_ho_so ?? null)
      setRecoStage('onboard')
      setRecoPayload(null)
      setRecoError('')
      setFeedbackNote('')
      if (typeof window !== 'undefined') {
        window.localStorage.setItem('mr_token', payload.token)
        window.localStorage.setItem('khangdauti_token', payload.token)
        window.localStorage.setItem('mr_profile_id', String(profiles[0]?.id_ho_so ?? ''))
        window.localStorage.setItem('khangdauti_user', JSON.stringify(payload.nguoi_dung))
      }
    },
    [buildApiUrl],
  )

  const handleLogin = useCallback(async () => {
    setAuthLoading(true)
    setAuthError('')
    try {
      await loginWithCredentials(loginIdentifier, loginPassword)
    } catch (error) {
      const message = formatApiError(error)
      setAuthError(message)
    } finally {
      setAuthLoading(false)
    }
  }, [formatApiError, loginIdentifier, loginPassword, loginWithCredentials])

  const handleDemoLogin = useCallback(async () => {
    setLoginIdentifier('demo')
    setLoginPassword('demo123')
    setAuthLoading(true)
    setAuthError('')
    try {
      await loginWithCredentials('demo', 'demo123')
    } catch (error) {
      const message = formatApiError(error)
      setAuthError(message)
    } finally {
      setAuthLoading(false)
    }
  }, [formatApiError, loginWithCredentials])

  const handleRegister = useCallback(async () => {
    if (!registerUsername.trim() || !registerEmail.trim() || !registerPassword.trim()) {
      setAuthError(locale === 'vi' ? 'Vui lòng điền đủ thông tin đăng ký.' : 'Please fill all required registration fields.')
      return
    }
    if (registerPassword !== registerConfirmPassword) {
      setAuthError(locale === 'vi' ? 'Mật khẩu xác nhận không khớp.' : 'Password confirmation does not match.')
      return
    }

    setAuthLoading(true)
    setAuthError('')
    try {
      const response = await fetch(buildApiUrl('/dang-ky'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          tenTaiKhoan: registerUsername.trim(),
          email: registerEmail.trim(),
          matKhau: registerPassword,
          hoTen: registerFullName.trim() || undefined,
        }),
      })
      if (!response.ok) {
        throw new Error((await response.text()) || `${response.status}`)
      }
      await loginWithCredentials(registerUsername.trim(), registerPassword)
      setAuthView('login')
      setRegisterFullName('')
      setRegisterUsername('')
      setRegisterEmail('')
      setRegisterPassword('')
      setRegisterConfirmPassword('')
    } catch (error) {
      const message = formatApiError(error)
      setAuthError(message)
    } finally {
      setAuthLoading(false)
    }
  }, [
    buildApiUrl,
    formatApiError,
    locale,
    loginWithCredentials,
    registerConfirmPassword,
    registerEmail,
    registerFullName,
    registerPassword,
    registerUsername,
  ])

  const handleLogout = useCallback(() => {
    clearSession()
    setAuthView('login')
    if (typeof window !== 'undefined') {
      window.location.href = '/auth'
    }
  }, [clearSession])

  const fetchHealth = useCallback(async () => {
    try {
      const response = await fetch(buildApiUrl('/health'))
      if (!response.ok) {
        throw new Error(`${response.status}`)
      }
      setApiHealth('online')
    } catch {
      setApiHealth('offline')
    }
  }, [buildApiUrl])

  const fetchRecommendations = useCallback(async () => {
    if (!isAuthenticated || !authToken || !activeProfileId) {
      setRecoError(locale === 'vi' ? 'Vui lòng đăng nhập và chọn hồ sơ trước khi lấy gợi ý.' : 'Please sign in and select a profile before requesting recommendations.')
      setRecoStage('onboard')
      return
    }

    setRecoLoading(true)
    setRecoError('')
    setFeedbackNote('')
    try {
      const params = new URLSearchParams()
      params.set('top_n', String(recoTopN))
      if (recoGenre.trim()) {
        params.set('genre', recoGenre.trim())
      }
      const response = await fetch(buildApiUrl(`/goi-y/${activeProfileId}?${params.toString()}`), {
        headers: { Authorization: `Bearer ${authToken}` },
      })
      if (!response.ok) {
        const errText = await response.text()
        throw new Error(errText || `${response.status}`)
      }
      const payload = (await response.json()) as RecommendationPayload
      setRecoPayload(payload)
      setRecoStage('discover')
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown API error'
      setRecoError(message)
      setRecoPayload(null)
    } finally {
      setRecoLoading(false)
    }
  }, [activeProfileId, authToken, buildApiUrl, isAuthenticated, locale, recoGenre, recoTopN])

  const postEvent = useCallback(
    async (eventType: 'click' | 'view' | 'skip' | 'rate', movieId: number, metadata: Record<string, unknown> = {}) => {
      await fetch(buildApiUrl('/event'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          eventType,
          userId: activeRecoUserId,
          movieId,
          metadata,
        }),
      })
    },
    [activeRecoUserId, buildApiUrl],
  )

  const submitRating = useCallback(
    async (movieId: number, rating: number) => {
      if (!isAuthenticated || !authToken || !activeProfileId) {
        setFeedbackNote(locale === 'vi' ? 'Vui lòng đăng nhập để gửi phản hồi.' : 'Please sign in to submit feedback.')
        return
      }

      setFeedbackNote('')
      try {
        const response = await fetch(buildApiUrl('/phan-hoi'), {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            Authorization: `Bearer ${authToken}`,
          },
          body: JSON.stringify({
            idHoSo: activeProfileId,
            idPhim: movieId,
            diemDanhGia: rating,
            retrain: false,
          }),
        })
        if (!response.ok) {
          const errText = await response.text()
          throw new Error(errText || `${response.status}`)
        }
        await postEvent('rate', movieId, { rating })
        setRecoStage('feedback')
        setFeedbackNote(
          locale === 'vi'
            ? 'Đã lưu đánh giá. Lần gợi ý kế tiếp sẽ cá nhân hóa tốt hơn.'
            : 'Rating saved. Next recommendations will improve.',
        )
      } catch (error) {
        const message = error instanceof Error ? error.message : 'Rate failed'
        setFeedbackNote(locale === 'vi' ? `Không lưu được đánh giá: ${message}` : `Could not save rating: ${message}`)
      }
    },
    [activeProfileId, authToken, buildApiUrl, isAuthenticated, locale, postEvent],
  )

  const toggleWatchlist = useCallback((movieId: number) => {
    setWatchlist((prev) => (prev.includes(movieId) ? prev.filter((id) => id !== movieId) : [...prev, movieId]))
  }, [])

  const scoreLabel = useCallback(
    (row: RecommendationRow, index: number, total: number) => {
      if (row.als_score != null && row.als_score <= 5) {
        return locale === 'vi'
          ? `Điểm dự đoán ${Number(row.als_score).toFixed(1)}/5`
          : `Predicted ${Number(row.als_score).toFixed(1)}/5`
      }
      const span = Math.max(total - 1, 1)
      const pct = Math.max(74, Math.round(94 - (index / span) * 18))
      if (pct >= 92) {
        return locale === 'vi' ? `Đề xuất nổi bật · ${pct}%` : `Top pick · ${pct}%`
      }
      if (pct >= 84) {
        return locale === 'vi' ? `Mức phù hợp cao · ${pct}%` : `High match · ${pct}%`
      }
      return locale === 'vi' ? `Mức phù hợp ${pct}%` : `Match level ${pct}%`
    },
    [locale],
  )

  const recommendationRows = useMemo(() => recoPayload?.recommendations ?? [], [recoPayload?.recommendations])

  const routeExplain = useMemo(() => {
    if (!isAuthenticated) {
      return locale === 'vi'
        ? 'Đăng nhập để mở hồ sơ cá nhân và nhận gợi ý phim theo tài khoản của bạn.'
        : 'Sign in to open personal profiles and receive account-based recommendations.'
    }

    const route = recoPayload?.route ?? ''
    if (route === 'als_legacy') {
      return locale === 'vi'
        ? 'Bạn đang nhận gợi ý cá nhân hóa từ lịch sử xem trước đó (ALS).'
        : 'History-based profile: served through the personalization route.'
    }
    if (route === 'hybrid') {
      return locale === 'vi'
        ? 'Hệ thống ưu tiên gợi ý cá nhân hóa dựa trên lịch sử đánh giá và gu thể loại (Hybrid).'
        : 'History-based profile: recommendations blend past behavior and genre affinity.'
    }
    if (route === 'fallback') {
      return locale === 'vi'
        ? 'Hồ sơ mới đang nhận gợi ý theo thể loại ưu tiên và độ phổ biến của phim (Cold Start).'
        : 'New profile: recommendations are generated from selected genre and popularity.'
    }
    if (userMode === 'existing') {
      return locale === 'vi'
        ? 'Bạn đang ở chế độ hồ sơ có lịch sử. Bấm "Lấy gợi ý phim" để nhận danh sách cá nhân hóa.'
        : 'You are in history-based profile mode. Click "Get movie recommendations" for personalized picks.'
    }
    if (userMode === 'new') {
      return locale === 'vi'
        ? 'Bạn đang ở chế độ hồ sơ mới. Hệ thống sẽ bắt đầu bằng gợi ý theo sở thích ban đầu.'
        : 'You are in new-profile mode. The system starts with genre and popularity-based picks.'
    }
    return locale === 'vi'
      ? 'Chọn hồ sơ và nhấn "Lấy gợi ý phim" để bắt đầu.'
      : 'Choose profile and click "Get Recommendations" to start the usage flow.'
  }, [isAuthenticated, locale, recoPayload?.route, userMode])

  const routeBadge = useMemo(() => {
    if (!isAuthenticated) {
      return locale === 'vi' ? 'Chưa đăng nhập' : 'Not signed in'
    }
    const route = recoPayload?.route ?? ''
    if (route === 'hybrid') {
      return locale === 'vi' ? 'Gợi ý cá nhân hóa (Hybrid)' : 'Personalized route (Hybrid)'
    }
    if (route === 'fallback') {
      return locale === 'vi' ? 'Gợi ý theo sở thích ban đầu' : 'Starter preference route'
    }
    if (route === 'als_legacy') {
      return locale === 'vi' ? 'Gợi ý cá nhân hóa (ALS)' : 'Personalized route (ALS)'
    }
    return locale === 'vi' ? 'Sẵn sàng tạo gợi ý' : 'Ready for recommendations'
  }, [isAuthenticated, locale, recoPayload?.route])

  const ratedMovieIds = useMemo(
    () =>
      Object.entries(ratingByMovie)
        .filter(([, score]) => score > 0)
        .map(([movieId]) => Number(movieId)),
    [ratingByMovie],
  )

  const ratedRows = useMemo(
    () => recommendationRows.filter((row) => ratedMovieIds.includes(row.movieId)),
    [recommendationRows, ratedMovieIds],
  )

  const watchlistRows = useMemo(
    () => recommendationRows.filter((row) => watchlist.includes(row.movieId)),
    [recommendationRows, watchlist],
  )

  const extractMovieYear = useCallback((title: string): string => {
    const matched = title.match(/\((\d{4})\)\s*$/)
    return matched?.[1] ?? ''
  }, [])

  const movieDetailText = useMemo(() => {
    if (!detailMovie) {
      return ''
    }
    return detailMovie.explain || (locale === 'vi' ? 'Đề xuất dựa trên tín hiệu hệ thống.' : 'Recommended from system signals.')
  }, [detailMovie, locale])

  const fallbackPoster = useCallback((movieId: number) => `https://picsum.photos/seed/movie-${movieId}/160/220`, [])

  const lookupPoster = useCallback(
    async (movie: RecommendationRow): Promise<string> => {
      const query = movie.title.replace(/\(\d{4}\)\s*$/g, '').trim()
      if (!query) {
        return fallbackPoster(movie.movieId)
      }
      try {
        const params = new URLSearchParams({
          term: query,
          media: 'movie',
          entity: 'movie',
          limit: '1',
        })
        const response = await fetch(`https://itunes.apple.com/search?${params.toString()}`)
        if (!response.ok) {
          return fallbackPoster(movie.movieId)
        }
        const payload = (await response.json()) as { results?: Array<{ artworkUrl100?: string }> }
        const artwork = payload.results?.[0]?.artworkUrl100
        if (!artwork) {
          return fallbackPoster(movie.movieId)
        }
        return artwork.replace('100x100bb.jpg', '600x900bb.jpg').replace('100x100bb', '600x900bb')
      } catch {
        return fallbackPoster(movie.movieId)
      }
    },
    [fallbackPoster],
  )

  const switchLocale = useCallback(
    (nextLocale: Locale) => {
      if (nextLocale === locale) {
        return
      }
      setLocale(nextLocale)
      setLocaleFlash(true)
      if (localeTimerRef.current !== null) {
        window.clearTimeout(localeTimerRef.current)
      }
      localeTimerRef.current = window.setTimeout(() => {
        setLocaleFlash(false)
      }, 420)
    },
    [locale],
  )

  useEffect(() => {
    if (reduceMotion) {
      return
    }
    const timers = [
      window.setTimeout(() => setIntroStep(1), 320),
      window.setTimeout(() => setIntroStep(2), 1180),
      window.setTimeout(() => setIntroStep(3), 2100),
    ]
    return () => {
      timers.forEach((timer) => window.clearTimeout(timer))
    }
  }, [reduceMotion])

  useEffect(() => {
    return () => {
      if (localeTimerRef.current !== null) {
        window.clearTimeout(localeTimerRef.current)
      }
    }
  }, [])

  useEffect(() => {
    void fetchHealth()
  }, [fetchHealth])

  useEffect(() => {
    const rows = recoPayload?.recommendations ?? []
    if (!rows.length) {
      return
    }
    const missing = rows.filter((row) => !posterByMovie[row.movieId])
    if (!missing.length) {
      return
    }
    let active = true
    void (async () => {
      const results = await Promise.all(
        missing.map(async (movie) => ({
          movieId: movie.movieId,
          poster: await lookupPoster(movie),
        })),
      )
      if (!active) {
        return
      }
      setPosterByMovie((prev) => {
        const next = { ...prev }
        results.forEach((item) => {
          next[item.movieId] = item.poster
        })
        return next
      })
    })()
    return () => {
      active = false
    }
  }, [lookupPoster, posterByMovie, recoPayload])

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            const sectionId = entry.target.getAttribute('data-scene') as SectionId | null
            if (sectionId) {
              setActiveSection(sectionId)
            }
          }
        }
      },
      { threshold: 0.45, rootMargin: '-14% 0px -34% 0px' },
    )

    SECTION_IDS.forEach((id) => {
      const section = sectionsRef.current[id]
      if (section) {
        observer.observe(section)
      }
    })
    return () => observer.disconnect()
  }, [pageMode])

  useEffect(() => {
    if (!pointerFine || reduceMotion) {
      return
    }

    const body = document.body
    body.classList.add('has-custom-cursor')
    const handleMove = (event: MouseEvent) => {
      setMouse({ x: event.clientX, y: event.clientY })
      setCursorVisible(true)
    }
    const handleLeave = (event: MouseEvent) => {
      const related = event.relatedTarget as Node | null
      if (related) {
        return
      }
      setCursorVisible(false)
    }

    const handleOver = (event: MouseEvent) => {
      const target = (event.target as HTMLElement | null)?.closest<HTMLElement>('[data-cursor-label]')
      setCursorLabel(target?.dataset.cursorLabel ?? '')
    }
    const handleOut = (event: MouseEvent) => {
      const target = (event.target as HTMLElement | null)?.closest<HTMLElement>('[data-cursor-label]')
      if (!target) {
        return
      }
      const related = event.relatedTarget as HTMLElement | null
      if (related?.closest('[data-cursor-label]')) {
        return
      }
      setCursorLabel('')
    }

    window.addEventListener('mousemove', handleMove)
    window.addEventListener('mouseout', handleLeave)
    window.addEventListener('mouseover', handleOver)
    window.addEventListener('mouseout', handleOut)
    return () => {
      body.classList.remove('has-custom-cursor')
      window.removeEventListener('mousemove', handleMove)
      window.removeEventListener('mouseout', handleLeave)
      window.removeEventListener('mouseover', handleOver)
      window.removeEventListener('mouseout', handleOut)
    }
  }, [pointerFine, reduceMotion])

  const addBurst = useCallback((x: number, y: number) => {
    const id = burstIdRef.current + 1
    burstIdRef.current = id
    setBursts((prev) => [...prev, { id, x, y }])
    window.setTimeout(() => {
      setBursts((prev) => prev.filter((burst) => burst.id !== id))
    }, 480)
  }, [])

  const handleBurst = useCallback(
    (event: ReactMouseEvent<HTMLElement>) => {
      addBurst(event.clientX, event.clientY)
    },
    [addBurst],
  )

  const flashScene = useCallback(() => {
    setSceneFlash(true)
    window.setTimeout(() => setSceneFlash(false), 340)
  }, [])

  const scrollToScene = useCallback(
    (id: SectionId) => {
      flashScene()
      const section = sectionsRef.current[id]
      if (!section) {
        return
      }
      const top = section.getBoundingClientRect().top + window.scrollY - 84
      window.scrollTo({ top, behavior: 'smooth' })
    },
    [flashScene],
  )

  const openRecommendPage = useCallback(
    (event?: ReactMouseEvent<HTMLElement>) => {
      if (event) {
        addBurst(event.clientX, event.clientY)
      }
      flashScene()
      setPageMode('recommend')
      setActiveSection('featured')
      if (typeof window !== 'undefined' && window.location.pathname !== '/recommend') {
        window.history.pushState({}, '', '/recommend')
      }
      window.scrollTo({ top: 0, behavior: 'smooth' })
    },
    [addBurst, flashScene],
  )

  const navigateScene = useCallback(
    (id: SectionId, event?: ReactMouseEvent<HTMLElement>) => {
      if (id === 'featured') {
        openRecommendPage(event)
        return
      }
      if (event) {
        addBurst(event.clientX, event.clientY)
      }
      if (pageMode === 'recommend') {
        setPageMode('landing')
        if (typeof window !== 'undefined' && window.location.pathname !== '/') {
          window.history.pushState({}, '', '/')
        }
        window.setTimeout(() => {
          scrollToScene(id)
        }, 50)
        return
      }
      scrollToScene(id)
    },
    [addBurst, openRecommendPage, pageMode, scrollToScene],
  )

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }
    const syncFromPath = () => {
      const recommendPath = window.location.pathname.startsWith('/recommend')
      setPageMode(recommendPath ? 'recommend' : 'landing')
      if (recommendPath) {
        setActiveSection('featured')
      }
    }
    syncFromPath()
    window.addEventListener('popstate', syncFromPath)
    return () => {
      window.removeEventListener('popstate', syncFromPath)
    }
  }, [])

  useEffect(() => {
    if (typeof window === 'undefined') {
      return
    }
    const savedToken = window.localStorage.getItem('mr_token') ?? window.localStorage.getItem('khangdauti_token') ?? ''
    const savedUserRaw = window.localStorage.getItem('khangdauti_user')
    if (!savedToken || !savedUserRaw) {
      return
    }
    try {
      const parsedUser = JSON.parse(savedUserRaw) as AuthUser
      setAuthToken(savedToken)
      setAuthUser(parsedUser)
      void loadProfiles(savedToken).catch(() => {
        clearSession()
      })
    } catch {
      clearSession()
    }
  }, [clearSession, loadProfiles])

  useEffect(() => {
    if (!activeProfile) {
      return
    }
    const inferredMode: UserMode =
      activeProfile.che_do_goi_y === 'ca_nhan_hoa' ? 'existing' : 'new'
    setUserMode(inferredMode)
    if (activeProfile.the_loai_uu_tien && !recoGenre.trim()) {
      const firstGenre = String(activeProfile.the_loai_uu_tien).split('|')[0]?.trim()
      if (firstGenre) {
        setRecoGenre(firstGenre)
      }
    }
  }, [activeProfile, recoGenre, setUserMode])

  useEffect(() => {
    if (!authProfiles.length) {
      return
    }
    const matched = authProfiles.some((profile) => profile.id_ho_so === activeProfileId)
    if (!matched) {
      setActiveProfileId(authProfiles[0].id_ho_so)
    }
  }, [activeProfileId, authProfiles])

  const sectionReveal = {
    hidden: { opacity: 0, y: 42 },
    visible: { opacity: 1, y: 0, transition: { duration: 0.75, ease: EASE_SMOOTH } },
  }

  const cardsReveal = {
    hidden: { opacity: 0, y: 26 },
    visible: (index: number) => ({
      opacity: 1,
      y: 0,
      transition: { duration: 0.62, delay: index * 0.08, ease: EASE_SMOOTH },
    }),
  }

  return (
    <div className="app-root relative min-h-screen overflow-x-clip bg-[#050505] text-[#f5f5f5]">
      <div aria-hidden className="grain-layer" />
      <div aria-hidden className="vignette-layer" />

      <AnimatePresence>
        {stageStep < 3 && (
          <motion.div
            className="intro-stage"
            initial={{ opacity: 1 }}
            animate={{ opacity: stageStep === 2 ? 0 : 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.7, ease: [0.4, 0, 0.2, 1] }}
          >
            <motion.div
              className="intro-pulse"
              initial={{ opacity: 0, scaleX: 0.86, filter: 'blur(4px)' }}
              animate={{ opacity: stageStep >= 1 ? 1 : 0.45, scaleX: 1, filter: 'blur(0px)' }}
              transition={{ duration: 0.8, ease: EASE_SMOOTH }}
            />
            <motion.p
              className="intro-label"
              initial={{ opacity: 0, y: 14 }}
              animate={{ opacity: stageStep >= 1 ? 1 : 0, y: 0 }}
              transition={{ delay: 0.18, duration: 0.5 }}
            >
              Entering cinematic scene
            </motion.p>
          </motion.div>
        )}
      </AnimatePresence>

      <AnimatePresence>
        {sceneFlash && (
          <motion.div
            className="scene-flash"
            initial={{ opacity: 0 }}
            animate={{ opacity: 0.22 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.34 }}
          />
        )}
      </AnimatePresence>
      <AnimatePresence>
        {localeFlash && (
          <motion.div
            className="locale-flash"
            initial={{ opacity: 0 }}
            animate={{ opacity: 0.22 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.36, ease: EASE_SMOOTH }}
          />
        )}
      </AnimatePresence>

      <header className="sticky top-0 z-40 border-y border-[rgba(255,255,255,0.06)] bg-[rgba(5,5,5,0.82)] backdrop-blur-xl">
        <div className="mx-auto flex w-full max-w-none items-center justify-between gap-4 px-3 py-3 md:px-6 xl:px-10 2xl:px-14">
          <button
            type="button"
            onMouseDown={handleBurst}
            onClick={(event) => navigateScene('home', event)}
            className="group inline-flex items-center gap-2 rounded-full border border-[rgba(221,180,31,0.25)] bg-[rgba(16,16,16,0.66)] px-3 py-1.5 text-left"
            data-cursor-label="HOME"
          >
            <span className="h-2.5 w-2.5 rounded-full bg-[var(--gold)] shadow-[0_0_16px_rgba(221,180,31,0.42)]" />
            <span className="text-[0.82rem] font-semibold tracking-[0.1em] text-[#f5f5f5] md:text-[0.88rem]">
              {copy.brand}
            </span>
          </button>

          <nav className="hidden flex-1 items-center justify-evenly px-3 lg:flex xl:px-6">
            {navItems.map((item) => {
              const isActive = activeSection === item.id
              const menuLabel = item.id === 'featured' ? (locale === 'vi' ? 'Gợi ý phim' : 'Recommend') : item.label
              return (
                <button
                  key={item.id}
                  type="button"
                  className={`nav-link ${isActive ? 'is-active' : ''}`}
                  onMouseDown={handleBurst}
                  onClick={(event) => navigateScene(item.id, event)}
                  data-cursor-label="OPEN"
                >
                  {menuLabel}
                  {(item.id === 'works' || item.id === 'services') && <ChevronDown size={12} />}
                </button>
              )
            })}
          </nav>

          <div className="flex items-center gap-2">
            <div className="lang-switch" data-cursor-label="LANG">
              <button
                type="button"
                className={`lang-option ${locale === 'vi' ? 'is-active' : ''}`}
                onMouseDown={handleBurst}
                onClick={() => switchLocale('vi')}
              >
                VI
              </button>
              <button
                type="button"
                className={`lang-option ${locale === 'en' ? 'is-active' : ''}`}
                onMouseDown={handleBurst}
                onClick={() => switchLocale('en')}
              >
                EN
              </button>
            </div>
            {isAuthenticated && authUser ? (
              <>
                <button
                  type="button"
                  className="auth-btn"
                  data-cursor-label="PROFILE"
                  onMouseDown={handleBurst}
                  onClick={(event) => openRecommendPage(event)}
                >
                  {authUser.ten_tai_khoan}
                </button>
                <button
                  type="button"
                  className="auth-btn auth-btn-primary"
                  data-cursor-label="SIGN OUT"
                  onMouseDown={handleBurst}
                  onClick={() => handleLogout()}
                >
                  {locale === 'vi' ? 'Đăng xuất' : 'Sign out'}
                </button>
              </>
            ) : (
              <>
                <button
                  type="button"
                  className="auth-btn"
                  data-cursor-label="SIGN IN"
                  onMouseDown={handleBurst}
                  onClick={(event) => {
                    setAuthView('login')
                    openRecommendPage(event)
                  }}
                >
                  {copy.signIn}
                </button>
                <button
                  type="button"
                  className="auth-btn auth-btn-primary"
                  data-cursor-label="SIGN UP"
                  onMouseDown={handleBurst}
                  onClick={(event) => {
                    setAuthView('register')
                    openRecommendPage(event)
                  }}
                >
                  {copy.signUp}
                </button>
              </>
            )}
          </div>
        </div>
      </header>

      <motion.main
        key={locale}
        className={`pb-28 pt-8 md:pt-10 ${
          pageMode === 'recommend' ? 'w-full max-w-none px-0' : 'mx-auto w-[min(96vw,1480px)] px-4 md:px-6'
        }`}
        initial={{ opacity: 0.72, y: 8, filter: 'blur(4px)' }}
        animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
        transition={{ duration: 0.42, ease: EASE_SMOOTH }}
      >
        {pageMode === 'landing' && (
          <>
        <motion.section
          ref={(node) => {
            sectionsRef.current.home = node
            heroRef.current = node
          }}
          data-scene="home"
          className="scene-section relative min-h-[90vh] scroll-mt-24 border-b border-[rgba(221,180,31,0.12)] pb-20"
          initial="hidden"
          whileInView="visible"
          viewport={{ amount: 0.3, once: true }}
          variants={sectionReveal}
          style={{ opacity: heroOpacity }}
        >
          <motion.div className="hero-stage" style={{ y: heroPanelY }}>
            <motion.p
              className="hero-kicker"
              initial={{ opacity: 0, y: 14 }}
              animate={{ opacity: stageStep >= 1 ? 1 : 0, y: stageStep >= 1 ? 0 : 14 }}
              transition={{ duration: 0.55, delay: 0.2 }}
            >
              {copy.heroKicker}
            </motion.p>
            <div className="hero-title-stage">
              <motion.div style={{ y: heroTitleY }}>
                <motion.h1
                  className="hero-title"
                  initial={{
                    opacity: 0,
                    y: 112,
                    rotateX: 66,
                    skewX: -7,
                    scaleY: 0.72,
                    scaleX: 1.08,
                    filter: 'blur(16px)',
                  }}
                  animate={{
                    opacity: stageStep >= 1 ? 1 : 0,
                    y: stageStep >= 1 ? 0 : 112,
                    rotateX: stageStep >= 1 ? 0 : 66,
                    skewX: stageStep >= 1 ? 0 : -7,
                    scaleY: stageStep >= 1 ? 1 : 0.72,
                    scaleX: stageStep >= 1 ? 1 : 1.08,
                    filter: stageStep >= 1 ? 'blur(0px)' : 'blur(16px)',
                  }}
                  transition={{ duration: 1.12, ease: EASE_SLAM, delay: 0.18 }}
                >
                  {copy.heroTitle}
                </motion.h1>
              </motion.div>
            </div>

            <motion.p
              className="hero-copy"
              initial={{ opacity: 0, y: 22 }}
              animate={{ opacity: stageStep >= 2 ? 1 : 0, y: stageStep >= 2 ? 0 : 22 }}
              transition={{ duration: 0.64, delay: 0.12 }}
            >
              {copy.heroCopy}
            </motion.p>

            <motion.div
              className="hero-actions"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: stageStep >= 2 ? 1 : 0, y: stageStep >= 2 ? 0 : 20 }}
              transition={{ duration: 0.58, delay: 0.2 }}
            >
              <button
                type="button"
                className="primary-cta"
                data-cursor-label="START"
                onMouseDown={handleBurst}
                onClick={(event) => navigateScene('featured', event)}
              >
                {copy.heroPrimaryCta}
              </button>
              <button
                type="button"
                className="secondary-cta"
                data-cursor-label="OPEN"
                onMouseDown={handleBurst}
                onClick={(event) => navigateScene('services', event)}
              >
                {copy.heroSecondaryCta}
              </button>
            </motion.div>

            <motion.div
              className="hero-stats"
              initial={{ opacity: 0, y: 18 }}
              animate={{ opacity: stageStep >= 2 ? 1 : 0, y: stageStep >= 2 ? 0 : 18 }}
              transition={{ duration: 0.62, delay: 0.3 }}
            >
              {stats.map((stat) => (
                <div key={stat.label} className="hero-stat-card">
                  <p className="hero-stat-value">{stat.value}</p>
                  <p className="hero-stat-label">{stat.label}</p>
                </div>
              ))}
            </motion.div>
          </motion.div>

          <motion.div
            className="hero-thumbnails"
            initial="hidden"
            animate={stageStep >= 2 ? 'visible' : 'hidden'}
            variants={{ visible: { transition: { delayChildren: 0.14, staggerChildren: 0.09 } } }}
          >
            {HERO_THUMBNAILS.map((item, index) => (
              <motion.article
                key={item.title}
                className="thumb-card"
                custom={index}
                variants={cardsReveal}
                whileHover={{ y: -4 }}
                transition={{ duration: 0.35 }}
              >
                <img src={item.image} alt={item.title} loading="lazy" />
                <div className="thumb-meta">
                  <p>{item.title}</p>
                  <span>{item.score}</span>
                </div>
              </motion.article>
            ))}
          </motion.div>
        </motion.section>

        <TransitionPlate label={copy.plateModules} />

        <motion.section
          ref={(node) => {
            sectionsRef.current.services = node
          }}
          data-scene="services"
          className="scene-section scroll-mt-24 border-b border-[rgba(255,255,255,0.06)] py-18"
          initial="hidden"
          whileInView="visible"
          viewport={{ amount: 0.25, once: true }}
          variants={sectionReveal}
        >
          <div className="section-head">
            <p className="section-kicker">{copy.servicesKicker}</p>
            <h2>{copy.servicesTitle}</h2>
          </div>
          <div className={`services-list ${focusedService >= 0 ? 'is-focused' : ''}`}>
            {services.map((service, index) => {
              const isActive = focusedService === index
              return (
                <motion.article
                  key={service.title}
                  custom={index}
                  variants={cardsReveal}
                  tabIndex={0}
                  className={`service-row ${isActive ? 'is-active' : 'is-dimmed'}`}
                  onMouseEnter={() => setFocusedService(index)}
                  onFocus={() => setFocusedService(index)}
                  onClick={() => setFocusedService(index)}
                  onMouseDown={handleBurst}
                  data-cursor-label="FOCUS"
                >
                  <span className="service-row-noise" aria-hidden />
                  <span className="service-row-gain" aria-hidden />
                  <div className="service-index-wrap">
                    <span className="service-index">{service.index}</span>
                    <motion.span
                      className="service-accent"
                      animate={
                        isActive
                          ? { opacity: 1, scale: 1, rotate: 0 }
                          : { opacity: 0.35, scale: 0.84, rotate: -20 }
                      }
                      transition={{ duration: 0.35, ease: EASE_SMOOTH }}
                    >
                      <Sparkles size={14} />
                    </motion.span>
                  </div>
                  <div className="service-copy">
                    <h3>{service.title}</h3>
                    <p>{service.description}</p>
                  </div>
                  <div className="service-thumb">
                    <img src={service.image} alt={service.title} loading="lazy" />
                  </div>
                </motion.article>
              )
            })}
          </div>
        </motion.section>

        <TransitionPlate label={locale === 'vi' ? 'GỢI Ý PHIM' : 'RECOMMEND'} />
          </>
        )}
        {pageMode === 'landing' ? (
          <motion.section
            ref={(node) => {
              sectionsRef.current.featured = node
            }}
            data-scene="featured"
            className="scene-section featured-scene scroll-mt-24 border-b border-[rgba(221,180,31,0.12)] py-18"
            initial="hidden"
            whileInView="visible"
            viewport={{ amount: 0.3, once: true }}
            variants={sectionReveal}
          >
            <div className="featured-card">
              <img
                src="https://images.unsplash.com/photo-1594909122845-11baa439b7bf?auto=format&fit=crop&w=1800&q=80"
                alt="Live movie recommendation"
                loading="lazy"
              />
              <div className="featured-overlay" />
              <div className="featured-content">
                <p className="section-kicker">{locale === 'vi' ? 'Mục giới thiệu' : 'Introduction'}</p>
                <h2>{copy.featuredTitle}</h2>
                <div className="recommend-intro-card">
                  <p>
                    {locale === 'vi'
                      ? 'Đây là phần giới thiệu ngắn. Bấm nút bên dưới để mở trang gợi ý phim riêng, nơi bạn có thể chạy full flow: user cũ/user mới, xem lý do gợi ý, chấm sao và lưu xem sau.'
                      : 'This is the introduction block. Open the dedicated recommendation page to run the full user flow: returning/new user route, explanation, rating, and watchlist actions.'}
                  </p>
                  <div className="recommend-actions">
                    <button
                      type="button"
                      className="primary-cta recommend-run-btn"
                      onMouseDown={handleBurst}
                      onClick={(event) => openRecommendPage(event)}
                    >
                      {locale === 'vi' ? 'Mở trang Gợi ý phim' : 'Open Recommendation Page'}
                    </button>
                    <button
                      type="button"
                      className="secondary-cta recommend-run-btn"
                      onMouseDown={handleBurst}
                      onClick={(event) => navigateScene('works', event)}
                    >
                      {locale === 'vi' ? 'Xem kịch bản demo' : 'See demo scenarios'}
                    </button>
                  </div>
                </div>
              </div>
              <button
                type="button"
                className="featured-cta"
                onMouseDown={handleBurst}
                onClick={(event) => openRecommendPage(event)}
                data-cursor-label="VIEW"
              >
                <ArrowUpRight size={18} />
              </button>
            </div>
          </motion.section>
        ) : (
          <RecommendationPageShell
            locale={locale}
            isAuthenticated={isAuthenticated}
            authView={authView}
            setAuthView={setAuthView}
            authUser={authUser}
            authProfiles={authProfiles}
            activeProfileId={activeProfileId}
            setActiveProfileId={setActiveProfileId}
            activeRecoUserId={activeRecoUserId}
            authLoading={authLoading}
            authError={authError}
            loginIdentifier={loginIdentifier}
            setLoginIdentifier={setLoginIdentifier}
            loginPassword={loginPassword}
            setLoginPassword={setLoginPassword}
            registerFullName={registerFullName}
            setRegisterFullName={setRegisterFullName}
            registerUsername={registerUsername}
            setRegisterUsername={setRegisterUsername}
            registerEmail={registerEmail}
            setRegisterEmail={setRegisterEmail}
            registerPassword={registerPassword}
            setRegisterPassword={setRegisterPassword}
            registerConfirmPassword={registerConfirmPassword}
            setRegisterConfirmPassword={setRegisterConfirmPassword}
            onLogin={handleLogin}
            onRegister={handleRegister}
            onDemoLogin={handleDemoLogin}
            onLogout={handleLogout}
            featuredRef={(node) => {
              sectionsRef.current.featured = node
              heroRef.current = node
            }}
            stageTabs={stageTabs}
            recoStage={recoStage}
            setRecoStage={setRecoStage}
            apiHealth={apiHealth}
            routeBadge={routeBadge}
            routeExplain={routeExplain}
            userMode={userMode}
            setUserMode={setUserMode}
            recoTopN={recoTopN}
            setRecoTopN={setRecoTopN}
            recoGenre={recoGenre}
            setRecoGenre={setRecoGenre}
            quickGenres={quickGenres}
            recoLoading={recoLoading}
            ratedMovieIds={ratedMovieIds}
            recommendationRows={recommendationRows}
            posterByMovie={posterByMovie}
            watchlist={watchlist}
            watchlistRows={watchlistRows}
            ratingByMovie={ratingByMovie}
            ratedRows={ratedRows}
            ratingPanelMovieId={ratingPanelMovieId}
            scoreLabel={scoreLabel}
            extractMovieYear={extractMovieYear}
            setDetailMovie={setDetailMovie}
            postEvent={postEvent}
            toggleWatchlist={toggleWatchlist}
            setFeedbackNote={setFeedbackNote}
            setRatingPanelMovieId={setRatingPanelMovieId}
            setRatingByMovie={setRatingByMovie}
            submitRating={submitRating}
            fetchRecommendations={fetchRecommendations}
            handleBurst={handleBurst}
            recoError={recoError}
            feedbackNote={feedbackNote}
          />
        )}
        {pageMode === 'landing' && (
          <>
        <TransitionPlate label={copy.plateScenarios} />

        <motion.section
          ref={(node) => {
            sectionsRef.current.works = node
          }}
          data-scene="works"
          className="scene-section scroll-mt-24 border-b border-[rgba(255,255,255,0.06)] py-18"
          initial="hidden"
          whileInView="visible"
          viewport={{ amount: 0.25, once: true }}
          variants={sectionReveal}
        >
          <div className="section-head">
            <p className="section-kicker">{copy.worksKicker}</p>
            <h2>{copy.worksTitle}</h2>
          </div>
          <div className={`works-grid ${focusedWork >= 0 ? 'is-focused' : ''}`}>
            {works.map((work, index) => {
              const isActive = focusedWork === index
              return (
                <motion.article
                  key={work.title}
                  custom={index}
                  variants={cardsReveal}
                  tabIndex={0}
                  className={`work-card ${work.dominant ? 'work-card-dominant' : ''} ${isActive ? 'is-active' : 'is-dimmed'}`}
                  whileHover={{ y: -4 }}
                  onMouseEnter={() => setFocusedWork(index)}
                  onFocus={() => setFocusedWork(index)}
                  onClick={() => setFocusedWork(index)}
                  onMouseDown={handleBurst}
                  data-cursor-label="VIEW"
                >
                  <span className="work-card-noise" aria-hidden />
                  <span className="work-card-gain" aria-hidden />
                  <div className="work-image-wrap">
                    <img src={work.image} alt={work.title} loading="lazy" />
                  </div>
                  <div className="work-body">
                    <div className="work-meta-line">
                      <span>{work.category}</span>
                      <span>{work.year}</span>
                    </div>
                    <h3>{work.title}</h3>
                    <p>{work.description}</p>
                  </div>
                </motion.article>
              )
            })}
          </div>
        </motion.section>

        <TransitionPlate label={copy.plateMetrics} />

        <motion.section
          ref={(node) => {
            sectionsRef.current.awards = node
          }}
          data-scene="awards"
          className="scene-section scroll-mt-24 border-b border-[rgba(255,255,255,0.06)] py-18"
          initial="hidden"
          whileInView="visible"
          viewport={{ amount: 0.25, once: true }}
          variants={sectionReveal}
        >
          <div className="awards-layout">
            <div className="awards-intro">
              <p className="section-kicker">{copy.awardsKicker}</p>
              <h2>{copy.awardsTitle}</h2>
              <p>{copy.awardsCopy}</p>
            </div>
            <div className="awards-list">
              {awards.map((award, index) => (
                <motion.article key={award.title} className="award-card" custom={index} variants={cardsReveal}>
                  <div className="award-head">
                    <span>{award.tag}</span>
                    <span>{award.year}</span>
                  </div>
                  <h3>{award.title}</h3>
                  <p>{award.description}</p>
                </motion.article>
              ))}
            </div>
          </div>
        </motion.section>

        <motion.section
          ref={(node) => {
            sectionsRef.current.about = node
          }}
          data-scene="about"
          className="scene-section scroll-mt-24 border-b border-[rgba(255,255,255,0.06)] py-18"
          initial="hidden"
          whileInView="visible"
          viewport={{ amount: 0.25, once: true }}
          variants={sectionReveal}
        >
          <div className="about-layout">
            <h2>{copy.aboutTitle}</h2>
            <div>
              <p>{copy.aboutCopy}</p>
              <div className="about-highlights">
                {copy.aboutHighlights.map((item) => (
                  <span key={item}>{item}</span>
                ))}
              </div>
            </div>
          </div>
        </motion.section>

        <motion.section
          ref={(node) => {
            sectionsRef.current.clients = node
          }}
          data-scene="clients"
          className="scene-section scroll-mt-24 border-b border-[rgba(255,255,255,0.06)] py-18"
          initial="hidden"
          whileInView="visible"
          viewport={{ amount: 0.25, once: true }}
          variants={sectionReveal}
        >
          <p className="section-kicker">{copy.clientsKicker}</p>
          <div className="clients-grid">
            {CLIENTS.map((client, index) => (
              <motion.div key={client} className="client-cell" custom={index} variants={cardsReveal}>
                {client}
              </motion.div>
            ))}
          </div>
        </motion.section>

        <motion.section
          ref={(node) => {
            sectionsRef.current.contact = node
          }}
          data-scene="contact"
          className="scene-section contact-scene scroll-mt-24 py-24"
          initial="hidden"
          whileInView="visible"
          viewport={{ amount: 0.28, once: true }}
          variants={sectionReveal}
        >
          <p className="section-kicker">{copy.contactKicker}</p>
          <h2>{copy.contactTitle}</h2>
          <p>{copy.contactCopy}</p>
          <a href={`mailto:${copy.contactEmail}`} className="contact-link" data-cursor-label="EMAIL" onMouseDown={handleBurst}>
            {copy.contactEmail}
          </a>
          <button
            type="button"
            className="primary-cta mt-2"
            onMouseDown={handleBurst}
            data-cursor-label="START"
          >
            {copy.contactButton}
          </button>
        </motion.section>
          </>
        )}

        <AnimatePresence>
          {detailMovie && (
            <motion.div
              className="movie-detail-modal"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setDetailMovie(null)}
            >
              <motion.div
                className="movie-detail-card"
                initial={{ y: 26, opacity: 0.7 }}
                animate={{ y: 0, opacity: 1 }}
                exit={{ y: 20, opacity: 0.6 }}
                transition={{ duration: 0.28 }}
                onClick={(event) => event.stopPropagation()}
              >
                <img
                  src={posterByMovie[detailMovie.movieId] ?? `https://picsum.photos/seed/movie-${detailMovie.movieId}/360/520`}
                  alt={detailMovie.title}
                  loading="lazy"
                />
                <div className="movie-detail-info">
                  <p className="section-kicker">{locale === 'vi' ? 'Chi tiết phim' : 'Movie detail'}</p>
                  <h3>{detailMovie.title}</h3>
                  <p className="movie-detail-genres">{detailMovie.genres}</p>
                  <p className="movie-detail-explain">{movieDetailText}</p>
                  <div className="movie-detail-actions">
                    <button
                      type="button"
                      className="recommend-chip-btn is-active"
                      onClick={() => toggleWatchlist(detailMovie.movieId)}
                    >
                      {locale === 'vi' ? 'Lưu xem sau' : 'Save later'}
                    </button>
                    <button type="button" className="recommend-chip-btn" onClick={() => setDetailMovie(null)}>
                      {locale === 'vi' ? 'Đóng' : 'Close'}
                    </button>
                  </div>
                </div>
              </motion.div>
            </motion.div>
          )}
        </AnimatePresence>
      </motion.main>

      <footer className="mt-4 border-t border-[rgba(255,255,255,0.08)] px-4 py-8 md:px-6 md:py-10">
        <div
          className={`flex items-center justify-between gap-3 text-[0.78rem] uppercase tracking-[0.18em] text-[var(--text-soft)] ${
            pageMode === 'recommend' ? 'recommend-content-rail' : 'mx-auto w-[min(96vw,1480px)]'
          }`}
        >
          <p>{copy.footerLeft}</p>
          <p>{copy.footerMid}</p>
          <p>{copy.footerRight}</p>
        </div>
      </footer>

      <AnimatePresence>
        {bursts.map((burst) => (
          <motion.span
            key={burst.id}
            className="click-burst"
            style={{ left: burst.x, top: burst.y } as CSSProperties}
            initial={{ opacity: 0.84, scale: 0.56 }}
            animate={{ opacity: 0, scale: 1.25 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.46, ease: EASE_SMOOTH }}
          >
            {Array.from({ length: 10 }).map((_, index) => {
              const angle = (Math.PI * 2 * index) / 10
              const dx = Math.cos(angle) * 26
              const dy = Math.sin(angle) * 26
              const style: CSSProperties & Record<'--dx' | '--dy', string> = {
                '--dx': `${dx}px`,
                '--dy': `${dy}px`,
              }
              return <i key={`${burst.id}-${index}`} className="burst-dot" style={style} />
            })}
          </motion.span>
        ))}
      </AnimatePresence>

      {!reduceMotion && pointerFine && (
        <>
          <motion.div
            className={`cursor-core ${cursorVisible ? 'is-visible' : ''}`}
            animate={{ x: mouse.x, y: mouse.y }}
            transition={{ type: 'tween', duration: 0.08, ease: 'linear' }}
          />
          <motion.div
            className={`cursor-accent ${cursorLabel ? 'is-hover' : ''} ${cursorVisible ? 'is-visible' : ''}`}
            animate={{ x: mouse.x, y: mouse.y, rotate: cursorLabel ? 42 : 0 }}
            transition={{ type: 'spring', stiffness: 260, damping: 24, mass: 0.36 }}
          >
            <Sparkles size={14} />
            {cursorLabel && <span>{cursorLabel}</span>}
          </motion.div>
        </>
      )}
    </div>
  )
}

export default App

