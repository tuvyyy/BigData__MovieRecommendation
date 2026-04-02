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
      'Nạp ratings và movies bằng schema rõ ràng, làm sạch dữ liệu, partition theo user bucket, lưu Parquet tối ưu cho huấn luyện lớn.',
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

function App() {
  const reduceMotion = useReducedMotion()
  const [locale, setLocale] = useState<Locale>('vi')
  const [introStep, setIntroStep] = useState<number>(0)
  const [activeSection, setActiveSection] = useState<SectionId>('home')
  const [focusedService, setFocusedService] = useState<number>(1)
  const [focusedWork, setFocusedWork] = useState<number>(0)
  const [localeFlash, setLocaleFlash] = useState<boolean>(false)
  const [bursts, setBursts] = useState<Burst[]>([])
  const [cursorVisible, setCursorVisible] = useState(false)
  const [cursorLabel, setCursorLabel] = useState<string>('')
  const [sceneFlash, setSceneFlash] = useState<boolean>(false)
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
  }, [])

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

  const navigateScene = useCallback(
    (id: SectionId, event?: ReactMouseEvent<HTMLElement>) => {
      if (event) {
        addBurst(event.clientX, event.clientY)
      }
      setSceneFlash(true)
      window.setTimeout(() => setSceneFlash(false), 340)

      const section = sectionsRef.current[id]
      if (!section) {
        return
      }
      const top = section.getBoundingClientRect().top + window.scrollY - 84
      window.scrollTo({ top, behavior: 'smooth' })
    },
    [addBurst],
  )

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
              return (
                <button
                  key={item.id}
                  type="button"
                  className={`nav-link ${isActive ? 'is-active' : ''}`}
                  onMouseDown={handleBurst}
                  onClick={(event) => navigateScene(item.id, event)}
                  data-cursor-label="OPEN"
                >
                  {item.label}
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
            <button
              type="button"
              className="auth-btn"
              data-cursor-label="SIGN IN"
              onMouseDown={handleBurst}
            >
              {copy.signIn}
            </button>
            <button
              type="button"
              className="auth-btn auth-btn-primary"
              data-cursor-label="SIGN UP"
              onMouseDown={handleBurst}
            >
              {copy.signUp}
            </button>
          </div>
        </div>
      </header>

      <motion.main
        key={locale}
        className="mx-auto w-[min(96vw,1480px)] px-4 pb-28 pt-8 md:px-6 md:pt-10"
        initial={{ opacity: 0.72, y: 8, filter: 'blur(4px)' }}
        animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
        transition={{ duration: 0.42, ease: EASE_SMOOTH }}
      >
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
                onClick={(event) => navigateScene('works', event)}
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

        <TransitionPlate label={copy.plateFlow} />

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
              alt="Hybrid recommendation route"
              loading="lazy"
            />
            <div className="featured-overlay" />
            <div className="featured-content">
              <p className="section-kicker">{copy.featuredKicker}</p>
              <h2>{copy.featuredTitle}</h2>
              <div className="featured-meta">
                <p>
                  <span>{copy.featuredInputLabel}</span>
                  {copy.featuredInputValue}
                </p>
                <p>
                  <span>{copy.featuredRouteLabel}</span>
                  {copy.featuredRouteValue}
                </p>
                <p>
                  <span>{copy.featuredOutputLabel}</span>
                  {copy.featuredOutputValue}
                </p>
                <p>
                  <span>{copy.featuredExplainLabel}</span>
                  {copy.featuredExplainValue}
                </p>
              </div>
            </div>
            <button type="button" className="featured-cta" onMouseDown={handleBurst} data-cursor-label="VIEW">
              <ArrowUpRight size={18} />
            </button>
          </div>
        </motion.section>

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
      </motion.main>

      <footer className="border-t border-[rgba(255,255,255,0.06)] px-4 py-7 md:px-6">
        <div className="mx-auto flex w-[min(96vw,1480px)] items-center justify-between gap-3 text-[0.78rem] uppercase tracking-[0.2em] text-[var(--text-soft)]">
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
